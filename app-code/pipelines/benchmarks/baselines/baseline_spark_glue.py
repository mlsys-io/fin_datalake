from __future__ import annotations

import time
from typing import Any, Dict, List

from loguru import logger

from pipelines.benchmarks.contracts import SYSTEM_SPARK_GLUE
from pipelines.benchmarks.shared import (
    build_benchmark_result,
    capture_trial_input,
    deserialize_json_payload,
    extract_headlines,
    heuristic_market_news_analysis,
    heuristic_signal_result,
    persist_signal_history_direct,
    serialize_json_payload,
)
from pipelines.market_pulse_ingest import DEFAULT_PROVIDER, DEFAULT_SYMBOL


def _build_ohlc_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    prepared: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows):
        prepared.append(
            {
                "_seq": idx,
                "timestamp": row.get("timestamp"),
                "close": float(row.get("close") or 0.0),
                "volume": float(row.get("volume") or 0.0),
            }
        )
    return prepared


def _compute_market_state_with_spark(spark: Any, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    from pyspark.sql import functions as F

    prepared_rows = _build_ohlc_rows(rows)
    if not prepared_rows:
        return {"trend_score": 0.0}

    df = spark.createDataFrame(prepared_rows).orderBy("_seq")

    summary_row = df.agg(
        F.count("*").alias("window_count"),
        F.avg("close").alias("avg_price"),
        F.avg("volume").alias("avg_volume"),
        F.min("close").alias("min_price"),
        F.max("close").alias("max_price"),
        F.sum("volume").alias("total_volume"),
        F.sum(F.col("close") * F.col("volume")).alias("price_volume_sum"),
    ).first()

    first_row = df.orderBy("_seq").first()
    last_row = df.orderBy(F.desc("_seq")).first()
    recent_5 = [float(row["close"]) for row in df.orderBy(F.desc("_seq")).limit(5).collect()]
    recent_20 = [float(row["close"]) for row in df.orderBy(F.desc("_seq")).limit(20).collect()]
    close_values = [float(row["close"]) for row in df.select("close").collect()]

    avg_price = float(summary_row["avg_price"] or 0.0)
    variance = 0.0
    if close_values:
        variance = sum((price - avg_price) ** 2 for price in close_values) / len(close_values)

    first_price = float(first_row["close"] or 0.0) if first_row else 0.0
    last_price = float(last_row["close"] or 0.0) if last_row else 0.0
    total_volume = float(summary_row["total_volume"] or 0.0)
    vwap = None
    if total_volume > 0:
        vwap = float(summary_row["price_volume_sum"] or 0.0) / total_volume

    sma_5 = sum(recent_5) / len(recent_5) if recent_5 else None
    sma_20 = sum(recent_20) / len(recent_20) if recent_20 else None
    price_return_pct = ((last_price - first_price) / first_price * 100.0) if first_price else None

    trend_score = 0.0
    if sma_5 is not None and sma_20 not in (None, 0.0):
        trend_score = max(min(((sma_5 - sma_20) / sma_20) * 50.0, 1.0), -1.0)
    elif price_return_pct is not None:
        trend_score = max(min(price_return_pct / 5.0, 1.0), -1.0)

    return {
        "window_count": int(summary_row["window_count"] or 0),
        "last_price": last_price,
        "avg_price": avg_price,
        "avg_volume": float(summary_row["avg_volume"] or 0.0),
        "min_price": float(summary_row["min_price"] or 0.0),
        "max_price": float(summary_row["max_price"] or 0.0),
        "price_return_pct": price_return_pct,
        "volatility_estimate": variance ** 0.5 if close_values else None,
        "vwap": vwap,
        "sma_5": sma_5,
        "sma_20": sma_20,
        "last_tick_at": last_row["timestamp"] if last_row else None,
        "trend_score": trend_score,
    }


def run_spark_glue_baseline(
    *,
    trial_input: dict[str, Any] | None = None,
    provider: str = DEFAULT_PROVIDER,
    symbol: str = DEFAULT_SYMBOL,
):
    """
    Spark plus manual glue-code baseline using the shared benchmark contract.

    Spark is used for distributed-style data shaping and market metric
    aggregation, while orchestration and downstream analysis remain explicit
    Python glue code on the driver.
    """
    logger.info("=== BASELINE: SPARK + GLUE CODE ===")
    total_started = time.perf_counter()

    if trial_input is None:
        trial_input = capture_trial_input(provider=provider, symbol=symbol)
    else:
        trial_input = dict(trial_input)

    spark = None
    try:
        from pyspark.sql import SparkSession

        ingest_started = time.perf_counter()
        spark = (
            SparkSession.builder.appName("MarketPulseSparkGlueBaseline")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

        news_rows = list(trial_input.get("news") or [])
        ohlc_rows = list(trial_input.get("ohlc") or [])
        news_df = spark.createDataFrame(
            [{"headline": row.get("headline", ""), "source": row.get("site"), "_seq": idx} for idx, row in enumerate(news_rows)]
            or [{"headline": "", "source": "", "_seq": 0}]
        )
        headlines = [
            str(row["headline"]).strip()
            for row in news_df.orderBy("_seq").select("headline").collect()
            if str(row["headline"]).strip()
        ]
        market_state = _compute_market_state_with_spark(spark, ohlc_rows)
        trial_input["market_state"] = market_state
        ingest_duration = time.perf_counter() - ingest_started

        signal_started = time.perf_counter()
        headlines_payload = serialize_json_payload(headlines)
        market_state_payload = serialize_json_payload(market_state)

        analyst_result = heuristic_market_news_analysis(extract_headlines([{"headline": item} for item in deserialize_json_payload(headlines_payload)]))
        analyst_payload = serialize_json_payload(analyst_result)
        signal = heuristic_signal_result(
            symbol=str(trial_input.get("symbol") or symbol),
            market_state=deserialize_json_payload(market_state_payload),
            analyst_result=deserialize_json_payload(analyst_payload),
        )
        signal_duration = time.perf_counter() - signal_started

        persistence_started = time.perf_counter()
        persistence_meta = persist_signal_history_direct(signal)
        persistence_meta["mode"] = "spark_glue_direct"
        persistence_meta["notes"] = "Spark used for data shaping and metric aggregation, with manual Python glue between stages."
        persistence_meta["glue_payload_bytes"] = len(headlines_payload) + len(market_state_payload) + len(analyst_payload)
        persistence_duration = time.perf_counter() - persistence_started

        total_duration = time.perf_counter() - total_started
        logger.success(f"Spark baseline result: {signal['action']} {signal['symbol']} in {total_duration:.2f}s")
        return build_benchmark_result(
            system_name=SYSTEM_SPARK_GLUE,
            trial_input=trial_input,
            signal=signal,
            timings={
                "ingest_duration_seconds": ingest_duration,
                "signal_duration_seconds": signal_duration,
                "persistence_duration_seconds": persistence_duration,
                "total_duration_seconds": total_duration,
            },
            persistence_meta=persistence_meta,
        )
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    run_spark_glue_baseline()
