from __future__ import annotations

from collections import deque
import json
import os
import time
from typing import Any, Dict, List

from loguru import logger

from etl.config import config
from etl.io.tasks.delta_lake_write_task import DeltaLakeWriteTask
from etl.io.tasks.risingwave_write_task import RisingWaveWriteTask
from pipelines.benchmarks.contracts import BenchmarkResult, BenchmarkTimings, BenchmarkTrialInput
from pipelines.market_pulse_ingest import (
    DEFAULT_PROVIDER,
    DEFAULT_PRICE_WINDOW_SIZE,
    DEFAULT_SYMBOL,
    DEFAULT_WEBSOCKET_URL,
    MarketNewsIngestTask,
    risingwave_signal_table,
    signal_history_uri,
)
from etl.io.sources.websocket import WebSocketSource


def capture_trial_input(*, provider: str = DEFAULT_PROVIDER, symbol: str = DEFAULT_SYMBOL) -> BenchmarkTrialInput:
    return capture_trial_input_local(provider=provider, symbol=symbol)


def _baseline_price_capture_timeout_seconds() -> int:
    return int(os.environ.get("BASELINE_PRICE_CAPTURE_TIMEOUT_SECONDS", "20"))


def _binance_trade_stream_url(symbol: str) -> str:
    override = str(os.environ.get("BASELINE_PRICE_WEBSOCKET_URL") or "").strip()
    if override:
        return override

    configured = str(os.environ.get("DEMO_PRICE_WEBSOCKET_URL") or config.WEBSOCKET_URL or "").strip()
    if configured:
        return configured

    normalized = str(symbol).strip().upper() or DEFAULT_SYMBOL
    if normalized.endswith("USD"):
        pair = f"{normalized[:-3]}USDT"
    else:
        pair = normalized
    return f"wss://stream.binance.com:443/ws/{pair.lower()}@trade"


def _fallback_price_window(symbol: str, window_size: int = DEFAULT_PRICE_WINDOW_SIZE) -> List[Dict[str, Any]]:
    now_ms = int(time.time() * 1000)
    base_price = 68400.0
    pattern = [-120, -90, -60, -35, -20, -10, 0, 12, 25, 40, 55, 70, 82, 95, 105, 116, 128, 141, 155, 170, 185, 198, 212, 228, 245]
    rows = []
    window_pattern = pattern[-window_size:]
    for index, offset in enumerate(window_pattern):
        rows.append(
            {
                "symbol": symbol,
                "close": round(base_price + offset, 2),
                "volume": round(1.1 + (index * 0.07), 4),
                "timestamp": now_ms - ((len(window_pattern) - index) * 60_000),
                "provider": "fallback",
            }
        )
    return rows


def _normalize_price_batch(batch: List[Dict[str, Any]], symbol: str) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for entry in batch:
        if not isinstance(entry, dict):
            continue
        if entry.get("e") == "trade":
            price = entry.get("p")
            volume = entry.get("q")
            timestamp = entry.get("T") or entry.get("E")
            row_symbol = str(entry.get("s") or symbol).upper()
        elif entry.get("data") and isinstance(entry.get("data"), dict):
            trade = entry["data"]
            price = trade.get("p")
            volume = trade.get("q")
            timestamp = trade.get("T") or trade.get("E")
            row_symbol = str(trade.get("s") or symbol).upper()
        else:
            price = entry.get("price") or entry.get("p") or entry.get("close")
            volume = entry.get("volume") or entry.get("q") or entry.get("v")
            timestamp = entry.get("timestamp") or entry.get("T") or int(time.time() * 1000)
            row_symbol = str(entry.get("symbol") or entry.get("s") or symbol).upper()

        if price is None:
            continue

        normalized.append(
            {
                "symbol": row_symbol,
                "close": float(price),
                "volume": float(volume or 0.0),
                "timestamp": int(timestamp),
                "provider": "live_stream",
            }
        )

    normalized.sort(key=lambda row: int(row.get("timestamp") or 0))
    return normalized


def capture_price_window_local(
    *,
    symbol: str = DEFAULT_SYMBOL,
    window_size: int = DEFAULT_PRICE_WINDOW_SIZE,
    minimum_rows: int = 20,
) -> Dict[str, Any]:
    websocket_url = _binance_trade_stream_url(symbol)
    source = WebSocketSource(
        url=websocket_url,
        batch_size=20,
        read_timeout=1.0,
    )
    rows_window: deque[Dict[str, Any]] = deque(maxlen=window_size)
    batch_count = 0
    tick_count = 0
    last_error = None

    try:
        started = time.time()
        with source.open() as reader:
            for raw_batch in reader.read_batch():
                rows = _normalize_price_batch(raw_batch, symbol)
                if not rows:
                    if time.time() - started >= _baseline_price_capture_timeout_seconds():
                        break
                    continue

                batch_count += 1
                tick_count += len(rows)
                rows_window.extend(rows)

                if len(rows_window) >= max(minimum_rows, window_size):
                    break
                if time.time() - started >= _baseline_price_capture_timeout_seconds():
                    break

        live_rows = list(rows_window)
        if len(live_rows) < minimum_rows:
            raise RuntimeError(
                f"Captured only {len(live_rows)} live price rows for {symbol}; need at least {minimum_rows}"
            )
        market_state = compute_market_state_from_ohlc(live_rows)
        market_state["tick_count"] = tick_count
        market_state["batch_count"] = batch_count
        market_state["mode"] = "live_stream"
        return {
            "records": live_rows,
            "market_state": market_state,
            "mode": "live_stream",
            "provider": "websocket_direct",
            "fallback_used": False,
            "table_uri": "",
            "stream_uri": "",
            "risingwave_table": None,
            "source_error": None,
            "service_meta": {},
            "metrics": market_state,
            "source": websocket_url,
        }
    except Exception as exc:
        last_error = str(exc)
        logger.warning(f"[BenchmarkShared] Live websocket capture failed for {symbol}. Using fallback price window: {exc}")

    fallback_rows = _fallback_price_window(symbol, window_size=window_size)
    market_state = compute_market_state_from_ohlc(fallback_rows)
    market_state["tick_count"] = len(fallback_rows)
    market_state["batch_count"] = 1
    market_state["mode"] = "fallback"
    return {
        "records": fallback_rows,
        "market_state": market_state,
        "mode": "fallback",
        "provider": "fallback",
        "fallback_used": True,
        "table_uri": "",
        "stream_uri": "",
        "risingwave_table": None,
        "source_error": last_error,
        "service_meta": {},
        "metrics": market_state,
        "source": websocket_url or DEFAULT_WEBSOCKET_URL,
    }


def capture_trial_input_local(*, provider: str = DEFAULT_PROVIDER, symbol: str = DEFAULT_SYMBOL) -> BenchmarkTrialInput:
    news_result = MarketNewsIngestTask(name="Benchmark Local News Ingest").local(provider=provider, symbol=symbol)
    price_result = capture_price_window_local(symbol=symbol, window_size=DEFAULT_PRICE_WINDOW_SIZE)
    ingest_result = {
        "symbol": symbol,
        "provider_requested": provider,
        "news": news_result["records"],
        "ohlc": price_result["records"],
        "market_state": price_result["market_state"],
        "news_meta": {
            "mode": news_result["mode"],
            "provider": news_result["provider"],
            "fallback_used": news_result["fallback_used"],
            "persisted": False,
            "table_uri": news_result["table_uri"],
            "persist_error": None,
            "source_error": news_result["source_error"],
        },
        "ohlc_meta": {
            "mode": price_result["mode"],
            "provider": price_result["provider"],
            "fallback_used": price_result["fallback_used"],
            "persisted": False,
            "table_uri": price_result["table_uri"],
            "stream_uri": price_result["stream_uri"],
            "risingwave_table": price_result["risingwave_table"],
            "persist_error": None,
            "source_error": price_result["source_error"],
            "service_status": {},
            "service_meta": {
                "source": price_result["source"],
                "mode": price_result["mode"],
                "last_error": price_result["source_error"],
            },
            "metrics": price_result["metrics"],
        },
        "table_uris": {
            "news": news_result["table_uri"],
            "ohlc": price_result["table_uri"],
            "ohlc_stream": price_result["stream_uri"],
        },
        "stream_tables": {
            "price_risingwave": price_result["risingwave_table"],
        },
    }
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


def llm_market_news_analysis(headlines: List[str], market_state: Dict[str, Any] | None = None) -> Dict[str, Any]:
    from sample_agents.market_analyst import MarketAnalystAgent

    analyst = MarketAnalystAgent(config={})
    llm = analyst._get_llm(model_name="gemini-2.5-flash", temperature=0.0, json_mode=True)
    if llm:
        try:
            return analyst._analyze_with_llm(headlines, dict(market_state or {}), llm)
        except Exception as exc:
            logger.warning(f"[BenchmarkShared] Market analyst LLM path failed, using heuristic fallback: {exc}")
    return analyst._heuristic_analysis(headlines)


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


def llm_strategy_signal_result(
    *,
    symbol: str,
    market_state: Dict[str, Any],
    analyst_result: Dict[str, Any],
) -> Dict[str, Any]:
    from sample_agents.strategy_agent import StrategyAgent

    strategy = StrategyAgent(config={})
    trend_score = compute_trend_score_from_state(market_state)
    sentiment_label = str(analyst_result.get("label", "neutral")).lower()
    sentiment_score = float(analyst_result.get("score", 0.0))

    llm = strategy._get_llm(temperature=0.1, json_mode=True)
    if llm:
        try:
            action, confidence = strategy._generate_signal_llm(
                symbol=str(symbol).upper(),
                trend_score=trend_score,
                sentiment_label=sentiment_label,
                sentiment_score=sentiment_score,
                market_state=dict(market_state),
                analyst_summary=str(analyst_result.get("summary", "")),
                llm=llm,
            )
        except Exception as exc:
            logger.warning(f"[BenchmarkShared] Strategy LLM path failed, using heuristic fallback: {exc}")
            action, confidence = strategy._generate_signal_heuristic(trend_score, sentiment_score)
    else:
        action, confidence = strategy._generate_signal_heuristic(trend_score, sentiment_score)

    return {
        "symbol": str(symbol).upper(),
        "action": action,
        "confidence": round(min(max(float(confidence), 0.0), 1.0), 2),
        "trend_score": round(trend_score, 2),
        "sentiment_label": sentiment_label,
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
