from __future__ import annotations

from typing import Any, Dict, List, Literal, TypedDict


ModeClassification = Literal["fully_live", "partial_fallback", "fully_fallback", "failed"]

SYSTEM_PLAIN = "plain_sequential"
SYSTEM_SPARK_GLUE = "spark_glue"
SYSTEM_MARKET_PULSE = "market_pulse_integrated"

STAGE_FIELD_MAP = {
    "total": "total_duration_seconds",
    "ingest": "ingest_duration_seconds",
    "signal": "signal_duration_seconds",
    "persistence": "persistence_duration_seconds",
}


class BenchmarkTimings(TypedDict):
    ingest_duration_seconds: float
    signal_duration_seconds: float
    persistence_duration_seconds: float
    total_duration_seconds: float


class BenchmarkTrialInput(TypedDict, total=False):
    symbol: str
    provider_requested: str
    news: List[Dict[str, Any]]
    ohlc: List[Dict[str, Any]]
    market_state: Dict[str, Any]
    news_meta: Dict[str, Any]
    ohlc_meta: Dict[str, Any]
    table_uris: Dict[str, Any]
    stream_tables: Dict[str, Any]


class BenchmarkResult(TypedDict, total=False):
    system_name: str
    success: bool
    error: str
    symbol: str
    provider_requested: str
    signal: Dict[str, Any]
    market_state: Dict[str, Any]
    news_meta: Dict[str, Any]
    ohlc_meta: Dict[str, Any]
    persistence_meta: Dict[str, Any]
    timings: BenchmarkTimings
