import argparse
import json
import time
from typing import Any

from loguru import logger

from pipelines.benchmarks.contracts import SYSTEM_PLAIN
from pipelines.benchmarks.shared import (
    build_benchmark_result,
    capture_trial_input_local,
    compute_market_state_from_ohlc,
    extract_headlines,
    llm_market_news_analysis,
    llm_strategy_signal_result,
    persist_signal_history_direct,
)
from pipelines.market_pulse_ingest import DEFAULT_PROVIDER, DEFAULT_SYMBOL


def run_plain_baseline(
    *,
    trial_input: dict[str, Any] | None = None,
    provider: str = DEFAULT_PROVIDER,
    symbol: str = DEFAULT_SYMBOL,
):
    """Sequential benchmark baseline using the shared benchmark contract."""
    logger.info("=== BASELINE: PLAIN SEQUENTIAL PYTHON ===")
    total_started = time.perf_counter()

    if trial_input is None:
        ingest_started = time.perf_counter()
        trial_input = capture_trial_input_local(provider=provider, symbol=symbol)
        ingest_duration = time.perf_counter() - ingest_started
    else:
        trial_input = dict(trial_input)
        ingest_duration = 0.0

    signal_started = time.perf_counter()
    market_state = dict(trial_input.get("market_state") or {})
    if not market_state:
        market_state = compute_market_state_from_ohlc(list(trial_input.get("ohlc") or []))
        trial_input["market_state"] = market_state

    headlines = extract_headlines(list(trial_input.get("news") or []))
    analyst_result = llm_market_news_analysis(headlines, market_state)
    signal = llm_strategy_signal_result(
        symbol=str(trial_input.get("symbol") or symbol),
        market_state=market_state,
        analyst_result=analyst_result,
    )
    signal_duration = time.perf_counter() - signal_started

    persistence_started = time.perf_counter()
    persistence_meta = persist_signal_history_direct(signal)
    persistence_meta["mode"] = "direct_in_process"
    persistence_meta["notes"] = (
        "Signal generated and persisted directly in a single-process baseline path. "
        "The analyst and strategy reasoning use the same local LLM prompt logic as the integrated agents, "
        "but without Ray-hosted deployment or remote delegation."
    )
    persistence_duration = time.perf_counter() - persistence_started

    total_duration = time.perf_counter() - total_started
    logger.success(f"Baseline Result: {signal['action']} {signal['symbol']} in {total_duration:.2f}s")
    return build_benchmark_result(
        system_name=SYSTEM_PLAIN,
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the plain sequential Market Pulse baseline.")
    parser.add_argument("--provider", default=DEFAULT_PROVIDER)
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--json", action="store_true", help="Print the final payload as JSON.")
    args = parser.parse_args()
    result = run_plain_baseline(provider=args.provider, symbol=args.symbol)
    if args.json:
        print(json.dumps(result, indent=2))
