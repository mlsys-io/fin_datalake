from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any, Dict, List

import requests
import ray.serve as serve
from loguru import logger
from prefect import flow
from prefect_ray.task_runners import RayTaskRunner

from etl.agents.context import get_context
from etl.agents.manager import list_fleet_state
from etl.config import config
from etl.core.base_task import BaseTask
from etl.io.tasks.delta_lake_write_task import DeltaLakeWriteTask
from etl.io.tasks.risingwave_write_task import RisingWaveWriteTask
from etl.runtime import ensure_ray, resolve_serve_response
from pipelines.market_pulse_ingest import (
    DEFAULT_PROVIDER,
    DEFAULT_SYMBOL,
    market_pulse_ingest,
    risingwave_price_table,
    risingwave_signal_table,
    signal_history_uri,
)
from pipelines.self_healing_demo import run_self_healing_demo
from sample_agents.market_analyst import MarketAnalystAgent
from sample_agents.strategy_agent import StrategyAgent

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
except ImportError:  # pragma: no cover
    Console = None
    Panel = None
    Table = None


MARKET_ANALYST_AGENT_NAME = str(os.environ.get("DEMO_MARKET_ANALYST_AGENT_NAME", "MarketAnalyst-1")).strip() or "MarketAnalyst-1"
STRATEGY_AGENT_NAME = str(os.environ.get("DEMO_STRATEGY_AGENT_NAME", "CryptoStrategy-1")).strip() or "CryptoStrategy-1"
GATEWAY_URL = str(os.environ.get("DEMO_GATEWAY_URL", "http://localhost:8000")).strip() or "http://localhost:8000"
GATEWAY_BEARER_TOKEN = str(os.environ.get("DEMO_GATEWAY_BEARER_TOKEN", "")).strip()


def _section(title: str) -> None:
    logger.info("\n" + "=" * 64)
    logger.info(title)
    logger.info("=" * 64)


def _build_headlines(rows: List[Dict[str, Any]]) -> List[str]:
    return [str(row.get("headline", "")).strip() for row in rows if str(row.get("headline", "")).strip()]


def _structured_signal_line(signal_result: Dict[str, Any]) -> str:
    action = str(signal_result.get("action", "HOLD")).upper()
    confidence = float(signal_result.get("confidence", 0.0)) * 100
    trend_score = float(signal_result.get("trend_score", 0.0))
    sentiment_score = float(signal_result.get("sentiment_score", 0.0))
    symbol = str(signal_result.get("symbol", DEFAULT_SYMBOL))
    return (
        f"{action} {symbol} | confidence={confidence:.0f}% "
        f"| trend={trend_score:.2f} | sentiment={sentiment_score:.2f}"
    )


def _signal_history_row(signal: Dict[str, Any]) -> Dict[str, Any]:
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


class EnsureDemoAgentsTask(BaseTask):
    def run(self) -> Dict[str, Any]:
        ensure_ray()
        MarketAnalystAgent.deploy(name=MARKET_ANALYST_AGENT_NAME)
        StrategyAgent.deploy(name=STRATEGY_AGENT_NAME)
        time.sleep(2)

        relevant_names = {MARKET_ANALYST_AGENT_NAME, STRATEGY_AGENT_NAME}
        fleet_state = list_fleet_state()
        relevant_agents = [agent for agent in fleet_state.get("merged", []) if agent.get("name") in relevant_names]
        return {
            "strategy_agent_name": STRATEGY_AGENT_NAME,
            "market_analyst_agent_name": MARKET_ANALYST_AGENT_NAME,
            "agent_state": relevant_agents,
        }


class GenerateSignalTask(BaseTask):
    def run(self, *, symbol: str, ingest_result: Dict[str, Any]) -> Dict[str, Any]:
        ensure_ray()

        ohlc_data = list(ingest_result.get("ohlc") or [])
        if not ohlc_data:
            raise RuntimeError("Signal workflow requires OHLC data, but ingest returned no rows.")

        payload = {
            "symbol": symbol,
            "ohlc_data": ohlc_data,
            "headlines": _build_headlines(list(ingest_result.get("news") or [])),
            "market_state": dict(ingest_result.get("market_state") or {}),
        }

        handle = serve.get_app_handle(STRATEGY_AGENT_NAME)
        started = time.perf_counter()
        signal_result = resolve_serve_response(handle.ask.remote(payload))
        duration_seconds = time.perf_counter() - started

        return {
            "signal": signal_result,
            "duration_seconds": duration_seconds,
            "payload_summary": {
                "headline_count": len(payload["headlines"]),
                "ohlc_rows": len(ohlc_data),
            },
        }


class PersistSignalHistoryTask(BaseTask):
    def run(self, *, signal: Dict[str, Any]) -> Dict[str, Any]:
        row = _signal_history_row(signal)
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
                ddl=(
                    f'CREATE TABLE IF NOT EXISTS "{config.RISINGWAVE_SCHEMA}"."{risingwave_signal_table()}" ('
                    "symbol VARCHAR, action VARCHAR, confidence DOUBLE PRECISION, trend_score DOUBLE PRECISION, "
                    "sentiment_label VARCHAR, sentiment_score DOUBLE PRECISION, analyst_summary VARCHAR, "
                    "last_price DOUBLE PRECISION, sma_5 DOUBLE PRECISION, sma_20 DOUBLE PRECISION, "
                    "vwap DOUBLE PRECISION, price_return_pct DOUBLE PRECISION, volatility_estimate DOUBLE PRECISION, "
                    "window_count INTEGER, timestamp_ms BIGINT)"
                ),
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


class CollectVisibilityTask(BaseTask):
    def run(self, *, symbol: str) -> Dict[str, Any]:
        signal_key = f"signal:{symbol}"
        saved_signal = None
        context_error = None

        try:
            context_store = get_context()
            saved_signal = context_store.get(signal_key)
        except Exception as exc:
            context_error = str(exc)

        relevant_names = {MARKET_ANALYST_AGENT_NAME, STRATEGY_AGENT_NAME}
        fleet_state = list_fleet_state()
        relevant_agents = [agent for agent in fleet_state.get("merged", []) if agent.get("name") in relevant_names]

        return {
            "signal_key": signal_key,
            "signal": saved_signal,
            "context_error": context_error,
            "agent_state": relevant_agents,
        }


@flow(name="market-pulse-demo", task_runner=RayTaskRunner(address=config.RAY_ADDRESS))
def market_pulse_demo_workflow(
    *,
    provider: str = DEFAULT_PROVIDER,
    symbol: str = DEFAULT_SYMBOL,
) -> Dict[str, Any]:
    workflow_started = time.perf_counter()

    logger.info("[MarketPulse] Running market ingest subflow")
    ingest_started = time.perf_counter()
    ingest_result = market_pulse_ingest(provider=provider, symbol=symbol)
    ingest_duration = time.perf_counter() - ingest_started

    logger.info("[MarketPulse] Ensuring demo agents are deployed")
    agent_setup_started = time.perf_counter()
    agent_result = EnsureDemoAgentsTask(name="Ensure Demo Agents").submit().result()
    agent_setup_duration = time.perf_counter() - agent_setup_started

    logger.info("[MarketPulse] Generating remote trading signal")
    signal_result = GenerateSignalTask(name="Generate Market Signal").submit(
        symbol=symbol,
        ingest_result=ingest_result,
    ).result()

    logger.info("[MarketPulse] Persisting signal history")
    persistence_started = time.perf_counter()
    persistence_result = PersistSignalHistoryTask(name="Persist Signal History").submit(
        signal=signal_result.get("signal", {}),
    ).result()
    persistence_duration = time.perf_counter() - persistence_started

    logger.info("[MarketPulse] Collecting visibility metadata")
    visibility_started = time.perf_counter()
    visibility_result = CollectVisibilityTask(name="Collect Demo Visibility").submit(symbol=symbol).result()
    visibility_duration = time.perf_counter() - visibility_started

    total_duration = time.perf_counter() - workflow_started

    return {
        "symbol": symbol,
        "provider_requested": provider,
        "signal": signal_result.get("signal", {}),
        "duration_seconds": total_duration,
        "total_duration_seconds": total_duration,
        "ingest_duration_seconds": ingest_duration,
        "agent_setup_duration_seconds": agent_setup_duration,
        "signal_duration_seconds": signal_result.get("duration_seconds", 0.0),
        "persistence_duration_seconds": persistence_duration,
        "visibility_duration_seconds": visibility_duration,
        "payload_summary": signal_result.get("payload_summary", {}),
        "market_state": ingest_result.get("market_state", {}),
        "news_meta": ingest_result.get("news_meta", {}),
        "ohlc_meta": ingest_result.get("ohlc_meta", {}),
        "signal_persistence": persistence_result,
        "table_uris": {
            **dict(ingest_result.get("table_uris", {})),
            "signals": signal_history_uri(),
        },
        "stream_tables": {
            **dict(ingest_result.get("stream_tables", {})),
            "signal_risingwave": f"{config.RISINGWAVE_SCHEMA}.{risingwave_signal_table()}",
        },
        "agent_state": visibility_result.get("agent_state") or agent_result.get("agent_state") or [],
        "visibility": visibility_result,
    }


def run_market_pulse_flow(*, provider: str = DEFAULT_PROVIDER, symbol: str = DEFAULT_SYMBOL) -> Dict[str, Any]:
    _section("SUBMITTING MARKET PULSE DEMO WORKFLOW")
    result = market_pulse_demo_workflow(provider=provider, symbol=symbol)
    logger.success(f"[MarketPulse] Workflow completed in {float(result.get('duration_seconds', 0.0)):.2f}s")
    logger.info(f"[MarketPulse] {_structured_signal_line(result.get('signal', {}))}")
    return result


def _gateway_headers(bearer_token: str) -> Dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    return headers


def _query_gateway_intent(*, gateway_url: str, bearer_token: str, action: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
    response = requests.post(
        f"{gateway_url.rstrip('/')}/api/v1/intent",
        json={"domain": "data", "action": action, "parameters": parameters},
        headers=_gateway_headers(bearer_token),
        timeout=15,
    )
    response.raise_for_status()
    return response.json()


def run_query_chunk(
    *,
    symbol: str = DEFAULT_SYMBOL,
    gateway_url: str = GATEWAY_URL,
    gateway_bearer_token: str = GATEWAY_BEARER_TOKEN,
) -> Dict[str, Any]:
    _section("[Query] Retrieve persisted results through Gateway")
    if not gateway_bearer_token:
        note = "No DEMO_GATEWAY_BEARER_TOKEN configured; gateway query chunk skipped."
        logger.warning(note)
        return {"skipped": True, "reason": note}

    signal_sql = (
        f"SELECT symbol, action, confidence, sentiment_label, sentiment_score, last_price, sma_5, sma_20, timestamp_ms "
        f"FROM {config.RISINGWAVE_SCHEMA}.{risingwave_signal_table()} "
        f"WHERE symbol = '{symbol}' ORDER BY timestamp_ms DESC LIMIT 5"
    )
    price_sql = (
        f"SELECT symbol, close, vwap, sma_5, sma_20, price_return_pct, timestamp_ms "
        f"FROM {config.RISINGWAVE_SCHEMA}.{risingwave_price_table()} "
        f"WHERE symbol = '{symbol}' ORDER BY timestamp_ms DESC LIMIT 5"
    )

    queries = {}
    for key, sql in {"signals": signal_sql, "prices": price_sql}.items():
        try:
            queries[key] = _query_gateway_intent(
                gateway_url=gateway_url,
                bearer_token=gateway_bearer_token,
                action="query_stream",
                parameters={"sql": sql},
            )
        except Exception as exc:
            queries[key] = {"success": False, "error_message": str(exc), "rows": [], "columns": []}
    return queries


def run_recovery_chunk() -> Dict[str, Any]:
    _section("[Recovery] Trigger and verify self-healing")
    run_self_healing_demo()
    return {"status": "completed"}


def _console() -> Any:
    return Console() if Console is not None else None


def _fmt_float(value: Any, *, digits: int = 2, suffix: str = "") -> str:
    try:
        return f"{float(value):.{digits}f}{suffix}"
    except (TypeError, ValueError):
        return "-"


def _fmt_percent(value: Any) -> str:
    try:
        return f"{float(value) * 100:.0f}%"
    except (TypeError, ValueError):
        return "-"


def _print_fallback_summary(result: Dict[str, Any]) -> None:
    signal = result.get("signal", {})
    logger.success(f"Market Pulse Result: {_structured_signal_line(signal)}")
    logger.info(f"Analyst: {signal.get('sentiment_label', '-')} ({signal.get('sentiment_score', '-')})")


def _render_rich_summary(result: Dict[str, Any]) -> None:
    console = _console()
    if console is None or Panel is None or Table is None:
        _print_fallback_summary(result)
        return

    signal = result.get("signal", {})
    market_state = result.get("market_state", {}) or {}
    news_meta = result.get("news_meta", {})
    ohlc_meta = result.get("ohlc_meta", {})
    persistence = result.get("signal_persistence", {})

    console.print(Panel(f"{_structured_signal_line(signal)}\nWorkflow duration: {_fmt_float(result.get('duration_seconds'), digits=2, suffix='s')}", title="Market Pulse Result", expand=False))

    signal_table = Table(title="Signal Summary", show_header=True, header_style="bold cyan")
    signal_table.add_column("Field")
    signal_table.add_column("Value")
    signal_table.add_row("Action", str(signal.get("action", "HOLD")).upper())
    signal_table.add_row("Confidence", _fmt_percent(signal.get("confidence")))
    signal_table.add_row("Trend Score", _fmt_float(signal.get("trend_score")))
    signal_table.add_row("Sentiment", f"{signal.get('sentiment_label', '-')} ({_fmt_float(signal.get('sentiment_score'))})")
    signal_table.add_row("Analyst Summary", str(signal.get("analyst_summary", "-")))
    console.print(signal_table)

    market_table = Table(title="Market State", show_header=True, header_style="bold green")
    market_table.add_column("Field")
    market_table.add_column("Value")
    market_table.add_row("News Mode", str(news_meta.get("mode", "-")))
    market_table.add_row("Price Mode", str(ohlc_meta.get("mode", "-")))
    market_table.add_row("Last Price", _fmt_float(market_state.get("last_price")))
    market_table.add_row("SMA 5", _fmt_float(market_state.get("sma_5")))
    market_table.add_row("SMA 20", _fmt_float(market_state.get("sma_20")))
    market_table.add_row("VWAP", _fmt_float(market_state.get("vwap")))
    market_table.add_row("Return %", _fmt_float(market_state.get("price_return_pct"), suffix="%"))
    market_table.add_row("Volatility", _fmt_float(market_state.get("volatility_estimate")))
    console.print(market_table)

    persist_table = Table(title="Persistence Targets", show_header=True, header_style="bold yellow")
    persist_table.add_column("Target")
    persist_table.add_column("Status")
    persist_table.add_column("Location")
    persist_table.add_row("Signal Delta", "ok" if persistence.get("delta", {}).get("persisted") else "error", str(persistence.get("delta", {}).get("table_uri", "-")))
    persist_table.add_row("Signal RisingWave", "ok" if persistence.get("risingwave", {}).get("persisted") else "error", str(persistence.get("risingwave", {}).get("table", "-")))
    persist_table.add_row("Price RisingWave", "ok", str(result.get("stream_tables", {}).get("price_risingwave", "-")))
    console.print(persist_table)


def _render_query_summary(result: Dict[str, Any]) -> None:
    console = _console()
    if console is None or Table is None:
        logger.info(f"Gateway query result: {json.dumps(result, default=str)[:500]}")
        return

    if result.get("skipped"):
        console.print(Panel(str(result.get("reason", "Skipped")), title="Gateway Query"))
        return

    for title, payload in result.items():
        table = Table(title=f"Gateway {title.title()} Query", show_header=True, header_style="bold magenta")
        columns = payload.get("columns", []) or []
        rows = payload.get("rows", []) or []
        if not columns:
            table.add_column("Status")
            table.add_row(str(payload.get("error_message", "No rows returned")))
        else:
            for column in columns:
                table.add_column(str(column))
            for row in rows[:5]:
                table.add_row(*[str(cell) for cell in row])
        console.print(table)


def run_demo(*, provider: str = DEFAULT_PROVIDER, symbol: str = DEFAULT_SYMBOL) -> Dict[str, Any]:
    return run_market_pulse_flow(provider=provider, symbol=symbol)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Market Pulse demo submitter and presenter.")
    parser.add_argument("--chunk", choices=["all", "main", "signal", "query", "recovery"], default="all")
    parser.add_argument("--provider", default=DEFAULT_PROVIDER)
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--json", action="store_true")
    return parser


def main() -> None:
    args = _build_parser().parse_args()

    if args.chunk == "recovery":
        result = run_recovery_chunk()
    elif args.chunk == "query":
        result = run_query_chunk(symbol=args.symbol)
        if not args.json:
            _render_query_summary(result)
    elif args.chunk in {"main", "signal"}:
        result = run_market_pulse_flow(provider=args.provider, symbol=args.symbol)
        if not args.json:
            _render_rich_summary(result)
    else:
        workflow_result = run_market_pulse_flow(provider=args.provider, symbol=args.symbol)
        query_result = run_query_chunk(symbol=args.symbol)
        result = {"workflow": workflow_result, "queries": query_result}
        if not args.json:
            _render_rich_summary(workflow_result)
            _render_query_summary(query_result)

    if args.json:
        print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Demo interrupted.")
