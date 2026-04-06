from __future__ import annotations

import json
import time
from typing import Any, Dict, List

from etl.config import config
from etl.io.tasks.delta_lake_write_task import DeltaLakeWriteTask
from etl.io.tasks.risingwave_write_task import RisingWaveWriteTask
from pipelines.benchmarks.contracts import BenchmarkResult, BenchmarkTimings, BenchmarkTrialInput
from pipelines.market_pulse_ingest import (
    DEFAULT_PROVIDER,
    DEFAULT_SYMBOL,
    market_pulse_ingest,
    risingwave_signal_table,
    signal_history_uri,
)


def capture_trial_input(*, provider: str = DEFAULT_PROVIDER, symbol: str = DEFAULT_SYMBOL) -> BenchmarkTrialInput:
    ingest_result = market_pulse_ingest(provider=provider, symbol=symbol)
    return build_trial_input(ingest_result, provider=provider, symbol=symbol)


def build_trial_input(
    ingest_result: Dict[str, Any],
    *,
    provider: str | None = None,
    symbol: str | None = None,
) -> BenchmarkTrialInput:
    return {
        "symbol": str(symbol or ingest_result.get("symbol") or DEFAULT_SYMBOL).upper(),
        "provider_requested": str(provider or ingest_result.get("provider_requested") or DEFAULT_PROVIDER),
        "news": list(ingest_result.get("news") or []),
        "ohlc": list(ingest_result.get("ohlc") or []),
        "market_state": dict(ingest_result.get("market_state") or {}),
        "news_meta": dict(ingest_result.get("news_meta") or {}),
        "ohlc_meta": dict(ingest_result.get("ohlc_meta") or {}),
        "table_uris": dict(ingest_result.get("table_uris") or {}),
        "stream_tables": dict(ingest_result.get("stream_tables") or {}),
    }


def extract_headlines(news_rows: List[Dict[str, Any]]) -> List[str]:
    return [str(row.get("headline", "")).strip() for row in news_rows if str(row.get("headline", "")).strip()]


def compute_market_state_from_ohlc(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    closes: List[float] = []
    volumes: List[float] = []
    timestamps: List[Any] = []

    for row in rows:
        try:
            closes.append(float(row.get("close")))
        except (TypeError, ValueError):
            continue
        try:
            volumes.append(float(row.get("volume") or 0.0))
        except (TypeError, ValueError):
            volumes.append(0.0)
        timestamps.append(row.get("timestamp"))

    if not closes:
        return {"trend_score": 0.0}

    sma_5 = sum(closes[-5:]) / min(len(closes), 5)
    sma_20 = sum(closes[-20:]) / min(len(closes), 20)
    avg_price = sum(closes) / len(closes)
    avg_volume = sum(volumes) / len(volumes) if volumes else 0.0
    total_volume = sum(volumes)
    vwap = (sum(price * volume for price, volume in zip(closes, volumes)) / total_volume) if total_volume > 0 else None
    first_price = closes[0]
    last_price = closes[-1]
    price_return_pct = ((last_price - first_price) / first_price * 100.0) if first_price else None

    volatility = None
    if len(closes) >= 2:
        variance = sum((price - avg_price) ** 2 for price in closes) / len(closes)
        volatility = variance ** 0.5

    state = {
        "window_count": len(closes),
        "last_price": last_price,
        "avg_price": avg_price,
        "avg_volume": avg_volume,
        "min_price": min(closes),
        "max_price": max(closes),
        "price_return_pct": price_return_pct,
        "volatility_estimate": volatility,
        "vwap": vwap,
        "sma_5": sma_5,
        "sma_20": sma_20,
        "last_tick_at": timestamps[-1] if timestamps else None,
    }
    state["trend_score"] = compute_trend_score_from_state(state)
    return state


def compute_trend_score_from_state(market_state: Dict[str, Any]) -> float:
    try:
        sma_5 = float(market_state.get("sma_5"))
        sma_20 = float(market_state.get("sma_20"))
        if sma_20 != 0:
            pct_diff = (sma_5 - sma_20) / sma_20
            return max(min(pct_diff * 50.0, 1.0), -1.0)
    except (TypeError, ValueError):
        pass

    try:
        return_pct = float(market_state.get("price_return_pct"))
        return max(min(return_pct / 5.0, 1.0), -1.0)
    except (TypeError, ValueError):
        return 0.0


def heuristic_market_news_analysis(headlines: List[str]) -> Dict[str, Any]:
    positive_words = {"surges", "jumps", "soars", "bullish", "growth", "profit", "beats", "rally", "inflows", "approval"}
    negative_words = {"plummets", "drops", "falls", "bearish", "loss", "misses", "crash", "lawsuit", "hack", "outflows"}

    breakdown = []
    scores: List[float] = []
    for headline in headlines:
        words = set(headline.lower().split())
        pos_count = len(words.intersection(positive_words))
        neg_count = len(words.intersection(negative_words))

        score = 0.0
        label = "neutral"
        if pos_count > neg_count:
            score = min(0.3 * (pos_count - neg_count), 0.9)
            label = "bullish"
        elif neg_count > pos_count:
            score = max(-0.3 * (neg_count - pos_count), -0.9)
            label = "bearish"

        scores.append(score)
        breakdown.append(
            {
                "headline": headline,
                "label": label,
                "score": round(score, 2),
            }
        )

    aggregate_score = sum(scores) / len(scores) if scores else 0.0
    aggregate_label = "neutral"
    if aggregate_score > 0.15:
        aggregate_label = "bullish"
    elif aggregate_score < -0.15:
        aggregate_label = "bearish"

    return {
        "label": aggregate_label,
        "score": round(aggregate_score, 2),
        "summary": f"Headline sentiment is {aggregate_label} based on {len(headlines)} recent articles.",
        "headlines": breakdown,
    }


def heuristic_signal_result(
    *,
    symbol: str,
    market_state: Dict[str, Any],
    analyst_result: Dict[str, Any],
) -> Dict[str, Any]:
    trend_score = compute_trend_score_from_state(market_state)
    sentiment_score = float(analyst_result.get("score", 0.0))
    final_score = (0.65 * trend_score) + (0.35 * sentiment_score)

    if final_score > 0.3:
        action = "BUY"
    elif final_score < -0.3:
        action = "SELL"
    else:
        action = "HOLD"

    return {
        "symbol": str(symbol).upper(),
        "action": action,
        "confidence": round(min(abs(final_score), 1.0), 2),
        "trend_score": round(trend_score, 2),
        "sentiment_label": str(analyst_result.get("label", "neutral")).lower(),
        "sentiment_score": round(sentiment_score, 2),
        "analyst_summary": str(analyst_result.get("summary", "")),
        "analyst_headlines": list(analyst_result.get("headlines") or []),
        "market_state": dict(market_state),
        "timestamp_ms": int(time.time() * 1000),
    }


def serialize_json_payload(payload: Any) -> str:
    return json.dumps(payload, default=str)


def deserialize_json_payload(payload: str) -> Any:
    return json.loads(payload)


def build_benchmark_result(
    *,
    system_name: str,
    trial_input: BenchmarkTrialInput,
    signal: Dict[str, Any],
    timings: BenchmarkTimings,
    persistence_meta: Dict[str, Any] | None = None,
    error: str = "",
) -> BenchmarkResult:
    return {
        "system_name": system_name,
        "success": not bool(error),
        "error": error,
        "symbol": str(trial_input.get("symbol") or DEFAULT_SYMBOL).upper(),
        "provider_requested": str(trial_input.get("provider_requested") or DEFAULT_PROVIDER),
        "signal": signal,
        "market_state": dict(trial_input.get("market_state") or {}),
        "news_meta": dict(trial_input.get("news_meta") or {}),
        "ohlc_meta": dict(trial_input.get("ohlc_meta") or {}),
        "persistence_meta": dict(persistence_meta or {}),
        "timings": timings,
    }


def signal_history_row(signal: Dict[str, Any]) -> Dict[str, Any]:
    market_state = dict(signal.get("market_state") or {})
    return {
        "symbol": str(signal.get("symbol", DEFAULT_SYMBOL)).upper(),
        "action": str(signal.get("action", "HOLD")).upper(),
        "confidence": float(signal.get("confidence", 0.0)),
        "trend_score": float(signal.get("trend_score", 0.0)),
        "sentiment_label": str(signal.get("sentiment_label", "neutral")).lower(),
        "sentiment_score": float(signal.get("sentiment_score", 0.0)),
        "analyst_summary": str(signal.get("analyst_summary", "")),
        "last_price": market_state.get("last_price"),
        "sma_5": market_state.get("sma_5"),
        "sma_20": market_state.get("sma_20"),
        "vwap": market_state.get("vwap"),
        "price_return_pct": market_state.get("price_return_pct"),
        "volatility_estimate": market_state.get("volatility_estimate"),
        "window_count": market_state.get("window_count"),
        "timestamp_ms": int(signal.get("timestamp_ms", int(time.time() * 1000))),
    }


def signal_risingwave_ddl() -> str:
    return (
        f'CREATE TABLE IF NOT EXISTS "{config.RISINGWAVE_SCHEMA}"."{risingwave_signal_table()}" ('
        "symbol VARCHAR, action VARCHAR, confidence DOUBLE PRECISION, trend_score DOUBLE PRECISION, "
        "sentiment_label VARCHAR, sentiment_score DOUBLE PRECISION, analyst_summary VARCHAR, "
        "last_price DOUBLE PRECISION, sma_5 DOUBLE PRECISION, sma_20 DOUBLE PRECISION, "
        "vwap DOUBLE PRECISION, price_return_pct DOUBLE PRECISION, volatility_estimate DOUBLE PRECISION, "
        "window_count INTEGER, timestamp_ms BIGINT)"
    )


def persist_signal_history_direct(signal: Dict[str, Any]) -> Dict[str, Any]:
    row = signal_history_row(signal)
    delta_error = None
    risingwave_error = None

    try:
        DeltaLakeWriteTask(
            name="Persist Signal History To Delta",
            uri=signal_history_uri(),
            mode="append",
        ).local([row])
    except Exception as exc:
        delta_error = str(exc)

    try:
        RisingWaveWriteTask(
            name="Persist Signal History To RisingWave",
            table_name=risingwave_signal_table(),
            ddl=signal_risingwave_ddl(),
        ).local([row])
    except Exception as exc:
        risingwave_error = str(exc)

    return {
        "delta": {
            "persisted": delta_error is None,
            "table_uri": signal_history_uri(),
            "error": delta_error,
        },
        "risingwave": {
            "persisted": risingwave_error is None,
            "table": f"{config.RISINGWAVE_SCHEMA}.{risingwave_signal_table()}",
            "error": risingwave_error,
        },
        "row": row,
    }
