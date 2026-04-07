from __future__ import annotations

from collections import deque
from typing import Any, Dict, List
from urllib.parse import urlencode
import os
import time

from loguru import logger
from prefect import flow
from prefect_ray.task_runners import RayTaskRunner

from etl.agents.context import get_context
from etl.config import config
from etl.core.base_service import ServiceTask
from etl.core.base_task import BaseTask
from etl.io.sources.rest_api import RestApiSource
from etl.io.sources.websocket import WebSocketSource
from etl.io.tasks.delta_lake_write_task import DeltaLakeWriteTask
from etl.io.tasks.risingwave_write_task import RisingWaveWriteTask
from etl.runtime import ensure_ray


DEFAULT_PROVIDER = str(os.environ.get("DEMO_MARKET_DATA_PROVIDER", "fmp")).strip().lower() or "fmp"
DEFAULT_SYMBOL = str(os.environ.get("DEMO_SYMBOL", "BTCUSD")).strip().upper() or "BTCUSD"
DEFAULT_PRICE_WINDOW_SIZE = int(os.environ.get("DEMO_PRICE_WINDOW_SIZE", "25"))
PRICE_SERVICE_NAME = str(os.environ.get("DEMO_PRICE_SERVICE_NAME", "MarketPriceIngestService-BTCUSD")).strip() or "MarketPriceIngestService-BTCUSD"
PRICE_WINDOW_CONTEXT_KEY = str(os.environ.get("DEMO_PRICE_WINDOW_CONTEXT_KEY", f"market_pulse:price_window:{DEFAULT_SYMBOL}")).strip() or f"market_pulse:price_window:{DEFAULT_SYMBOL}"
PRICE_META_CONTEXT_KEY = str(os.environ.get("DEMO_PRICE_META_CONTEXT_KEY", f"market_pulse:price_meta:{DEFAULT_SYMBOL}")).strip() or f"market_pulse:price_meta:{DEFAULT_SYMBOL}"
PRICE_METRICS_CONTEXT_KEY = str(os.environ.get("DEMO_PRICE_METRICS_CONTEXT_KEY", f"market_pulse:price_metrics:{DEFAULT_SYMBOL}")).strip() or f"market_pulse:price_metrics:{DEFAULT_SYMBOL}"
DEFAULT_WEBSOCKET_URL = str(os.environ.get("DEMO_PRICE_WEBSOCKET_URL") or config.WEBSOCKET_URL or "wss://stream.binance.com:443/ws/btcusdt@trade").strip()
PRICE_SERVICE_READY_TIMEOUT_SECONDS = int(os.environ.get("DEMO_PRICE_SERVICE_READY_TIMEOUT_SECONDS", "20"))
PRICE_STREAM_RISINGWAVE_TABLE = str(os.environ.get("DEMO_RISINGWAVE_PRICE_TABLE", "market_pulse_prices")).strip() or "market_pulse_prices"
SIGNAL_RISINGWAVE_TABLE = str(os.environ.get("DEMO_RISINGWAVE_SIGNAL_TABLE", "market_pulse_signals")).strip() or "market_pulse_signals"

FMP_API_KEY = str(os.environ.get("FMP_API_KEY", "")).strip()


def market_pulse_root() -> str:
    return f"{config.DELTA_ROOT.rstrip('/')}/demo/market_pulse"


def news_table_uri() -> str:
    return f"{market_pulse_root()}/news"


def ohlc_table_uri() -> str:
    return f"{market_pulse_root()}/ohlc"


def ohlc_stream_uri() -> str:
    return f"{market_pulse_root()}/ohlc_stream"


def signal_history_uri() -> str:
    return f"{market_pulse_root()}/signals"


def risingwave_price_table() -> str:
    return PRICE_STREAM_RISINGWAVE_TABLE


def risingwave_signal_table() -> str:
    return SIGNAL_RISINGWAVE_TABLE


def _price_risingwave_identifier() -> str:
    return f"{config.RISINGWAVE_SCHEMA}.{PRICE_STREAM_RISINGWAVE_TABLE}"


def _signal_risingwave_identifier() -> str:
    return f"{config.RISINGWAVE_SCHEMA}.{SIGNAL_RISINGWAVE_TABLE}"


def _price_risingwave_ddl() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS "{config.RISINGWAVE_SCHEMA}"."{PRICE_STREAM_RISINGWAVE_TABLE}" (
        symbol VARCHAR,
        close DOUBLE PRECISION,
        volume DOUBLE PRECISION,
        timestamp_ms BIGINT,
        provider VARCHAR,
        mode VARCHAR,
        window_count INTEGER,
        last_price DOUBLE PRECISION,
        avg_price DOUBLE PRECISION,
        avg_volume DOUBLE PRECISION,
        min_price DOUBLE PRECISION,
        max_price DOUBLE PRECISION,
        price_return_pct DOUBLE PRECISION,
        volatility_estimate DOUBLE PRECISION,
        vwap DOUBLE PRECISION,
        sma_5 DOUBLE PRECISION,
        sma_20 DOUBLE PRECISION,
        trend_score DOUBLE PRECISION,
        last_tick_at DOUBLE PRECISION
    )
    """


def _signal_risingwave_ddl() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS "{config.RISINGWAVE_SCHEMA}"."{SIGNAL_RISINGWAVE_TABLE}" (
        symbol VARCHAR,
        action VARCHAR,
        confidence DOUBLE PRECISION,
        trend_score DOUBLE PRECISION,
        sentiment_label VARCHAR,
        sentiment_score DOUBLE PRECISION,
        analyst_summary VARCHAR,
        last_price DOUBLE PRECISION,
        sma_5 DOUBLE PRECISION,
        sma_20 DOUBLE PRECISION,
        vwap DOUBLE PRECISION,
        price_return_pct DOUBLE PRECISION,
        volatility_estimate DOUBLE PRECISION,
        window_count INTEGER,
        timestamp_ms BIGINT
    )
    """


class MarketNewsIngestTask(BaseTask):
    def __init__(self, name: str = "Ingest Market News", **kwargs):
        super().__init__(name=name, **kwargs)

    def _candidate_symbols(self, symbol: str) -> List[str]:
        normalized = str(symbol).strip().upper() or DEFAULT_SYMBOL
        candidates = [normalized]
        if normalized.endswith("USD"):
            base = normalized[:-3]
            candidates.extend([f"{normalized}T", base, f"{base}-USD"])
        if normalized.endswith("USDT"):
            base = normalized[:-4]
            candidates.extend([base, f"{base}USD"])

        unique: List[str] = []
        for item in candidates:
            cleaned = item.strip().upper()
            if cleaned and cleaned not in unique:
                unique.append(cleaned)
        return unique

    def _is_crypto_symbol(self, symbol: str) -> bool:
        normalized = str(symbol).strip().upper()
        return normalized.endswith(("USD", "USDT", "USDC", "BTC", "ETH"))

    def _fallback_records(self, symbol: str) -> List[Dict[str, Any]]:
        now = int(time.time())
        return [
            {
                "symbol": symbol,
                "headline": "Bitcoin ETFs attract renewed inflows as macro sentiment improves.",
                "source": "demo-fallback",
                "url": "https://example.invalid/demo/bitcoin-etf-inflows",
                "published_at": now - 900,
                "provider": "fallback",
            },
            {
                "symbol": symbol,
                "headline": "Crypto markets steady after volatility as traders await the next catalyst.",
                "source": "demo-fallback",
                "url": "https://example.invalid/demo/crypto-volatility",
                "published_at": now - 600,
                "provider": "fallback",
            },
            {
                "symbol": symbol,
                "headline": "Miners and exchanges report stable volumes despite short-term pullbacks.",
                "source": "demo-fallback",
                "url": "https://example.invalid/demo/miner-exchange-volumes",
                "published_at": now - 300,
                "provider": "fallback",
            },
        ]

    def _normalize_records(self, items: Any, *, symbol: str, provider: str) -> List[Dict[str, Any]]:
        if isinstance(items, dict):
            for key in ("news", "data", "results", "items"):
                if isinstance(items.get(key), list):
                    items = items[key]
                    break
            else:
                items = [items]

        normalized: List[Dict[str, Any]] = []
        if not isinstance(items, list):
            return normalized

        for entry in items:
            if not isinstance(entry, dict):
                continue

            headline = (
                entry.get("headline")
                or entry.get("title")
                or entry.get("text")
                or entry.get("summary")
                or ""
            )
            if not headline:
                continue

            published_at = (
                entry.get("publishedDate")
                or entry.get("published_at")
                or entry.get("datetime")
                or entry.get("date")
                or int(time.time())
            )
            normalized.append(
                {
                    "symbol": symbol,
                    "headline": str(headline).strip(),
                    "source": str(entry.get("site") or entry.get("source") or entry.get("publisher") or provider),
                    "url": str(entry.get("url") or entry.get("link") or ""),
                    "published_at": published_at,
                    "provider": provider,
                }
            )
            if len(normalized) >= 8:
                break
        return normalized

    def _read_rest_source(self, source: RestApiSource) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        with source.open() as reader:
            for batch in reader.read_batch():
                if isinstance(batch, list):
                    rows.extend(item for item in batch if isinstance(item, dict))
                elif isinstance(batch, dict):
                    rows.append(batch)
        return rows

    def _build_fmp_sources(self, symbol: str) -> List[RestApiSource]:
        if not FMP_API_KEY:
            raise RuntimeError("FMP_API_KEY is not configured")
        base_url = (
            "https://financialmodelingprep.com/stable/news/crypto"
            if self._is_crypto_symbol(symbol)
            else "https://financialmodelingprep.com/stable/news/stock"
        )
        return [
            RestApiSource(
                url=f"{base_url}?{urlencode({'symbols': candidate, 'limit': 8, 'apikey': FMP_API_KEY})}",
                method="GET",
                retries=2,
                rate_limit_delay=0.1,
            )
            for candidate in self._candidate_symbols(symbol)
        ]

    def _fetch_records(self, provider: str, symbol: str) -> tuple[List[Dict[str, Any]], str, str]:
        requested = str(provider).strip().lower() or DEFAULT_PROVIDER
        if requested != "fmp":
            raise ValueError(f"Unsupported demo provider: {requested}")

        for source in self._build_fmp_sources(symbol):
            normalized = self._normalize_records(self._read_rest_source(source), symbol=symbol, provider="fmp")
            if normalized:
                return normalized, "live_fmp", "fmp"
        raise RuntimeError("FMP news returned no rows")

    def run(self, provider: str = DEFAULT_PROVIDER, symbol: str = DEFAULT_SYMBOL) -> Dict[str, Any]:
        logger.info(f"[Ingest] Starting news ingest for {symbol} using provider='{provider}'")
        try:
            rows, mode, resolved_provider = self._fetch_records(provider, symbol)
            fallback_used = False
            source_error = None
        except Exception as exc:
            logger.warning(f"[Ingest] News provider '{provider}' unavailable. Falling back to deterministic sample data: {exc}")
            rows = self._fallback_records(symbol)
            mode = "fallback"
            resolved_provider = "fallback"
            fallback_used = True
            source_error = str(exc)

        return {
            "records": rows,
            "mode": mode,
            "provider": resolved_provider,
            "fallback_used": fallback_used,
            "table_uri": news_table_uri(),
            "source_error": source_error,
        }


class MarketPriceIngestService(ServiceTask):
    """Long-lived Ray service that ingests trades, maintains state, and writes to Delta + RisingWave."""

    def __init__(self, name: str | None = None, config: Dict[str, Any] | None = None, **kwargs):
        super().__init__(name=name, config=config, **kwargs)
        cfg = config or {}
        self.symbol = str(cfg.get("symbol", DEFAULT_SYMBOL)).strip().upper() or DEFAULT_SYMBOL
        self.window_size = int(cfg.get("window_size", DEFAULT_PRICE_WINDOW_SIZE))
        self.context_key = str(cfg.get("context_key", PRICE_WINDOW_CONTEXT_KEY)).strip() or PRICE_WINDOW_CONTEXT_KEY
        self.meta_key = str(cfg.get("meta_key", PRICE_META_CONTEXT_KEY)).strip() or PRICE_META_CONTEXT_KEY
        self.metrics_key = str(cfg.get("metrics_key", PRICE_METRICS_CONTEXT_KEY)).strip() or PRICE_METRICS_CONTEXT_KEY
        self.websocket_url = str(cfg.get("websocket_url", DEFAULT_WEBSOCKET_URL)).strip() or DEFAULT_WEBSOCKET_URL
        self.read_timeout = float(cfg.get("read_timeout", 1.0))
        self.batch_size = int(cfg.get("batch_size", 20))
        self.delta_uri = str(cfg.get("delta_uri", ohlc_stream_uri())).strip() or ohlc_stream_uri()
        self.risingwave_table = str(cfg.get("risingwave_table", PRICE_STREAM_RISINGWAVE_TABLE)).strip() or PRICE_STREAM_RISINGWAVE_TABLE
        self.window = deque(maxlen=self.window_size)
        self.last_write_count = 0
        self.last_ingest_mode = "starting"
        self.last_error = None
        self.last_tick_at = None
        self.total_ticks_processed = 0
        self.total_batches_processed = 0
        self.metrics_snapshot = self._empty_metrics()

    def _empty_metrics(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "window_count": 0,
            "tick_count": 0,
            "batch_count": 0,
            "last_price": None,
            "avg_price": None,
            "avg_volume": None,
            "min_price": None,
            "max_price": None,
            "price_return_pct": None,
            "volatility_estimate": None,
            "vwap": None,
            "sma_5": None,
            "sma_20": None,
            "trend_score": 0.0,
            "last_tick_at": None,
            "mode": self.last_ingest_mode,
        }

    def _fallback_window(self) -> List[Dict[str, Any]]:
        now_ms = int(time.time() * 1000)
        base_price = 68400.0
        pattern = [-120, -90, -60, -35, -20, -10, 0, 12, 25, 40, 55, 70, 82, 95, 105, 116, 128, 141, 155, 170, 185, 198, 212, 228, 245]
        rows = []
        for index, offset in enumerate(pattern[-self.window_size:]):
            rows.append(
                {
                    "symbol": self.symbol,
                    "close": round(base_price + offset, 2),
                    "volume": round(1.1 + (index * 0.07), 4),
                    "timestamp": now_ms - ((len(pattern) - index) * 60_000),
                    "provider": "fallback",
                }
            )
        return rows

    def _normalize_batch(self, batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for entry in batch:
            if not isinstance(entry, dict):
                continue
            if entry.get("e") == "trade":
                price = entry.get("p")
                volume = entry.get("q")
                timestamp = entry.get("T") or entry.get("E")
                symbol = str(entry.get("s") or self.symbol).upper()
            elif entry.get("data") and isinstance(entry.get("data"), dict):
                trade = entry["data"]
                price = trade.get("p")
                volume = trade.get("q")
                timestamp = trade.get("T") or trade.get("E")
                symbol = str(trade.get("s") or self.symbol).upper()
            else:
                price = entry.get("price") or entry.get("p") or entry.get("close")
                volume = entry.get("volume") or entry.get("q") or entry.get("v")
                timestamp = entry.get("timestamp") or entry.get("T") or int(time.time() * 1000)
                symbol = str(entry.get("symbol") or entry.get("s") or self.symbol).upper()

            if price is None:
                continue

            normalized.append(
                {
                    "symbol": symbol,
                    "close": float(price),
                    "volume": float(volume or 0.0),
                    "timestamp": int(timestamp),
                    "provider": "live_stream",
                }
            )

        normalized.sort(key=lambda row: int(row.get("timestamp") or 0))
        return normalized

    def _publish_window(self) -> None:
        store = get_context()
        window_rows = list(self.window)
        meta = {
            "symbol": self.symbol,
            "window_size": len(window_rows),
            "requested_window_size": self.window_size,
            "mode": self.last_ingest_mode,
            "source": self.websocket_url,
            "service_name": self.name,
            "metrics_key": self.metrics_key,
            "last_tick_at": self.last_tick_at,
            "last_write_count": self.last_write_count,
            "last_error": self.last_error,
            "risingwave_table": _price_risingwave_identifier(),
        }
        store.set(self.context_key, window_rows, ttl=900, owner=self.name)
        store.set(self.meta_key, meta, ttl=900, owner=self.name)
        store.set(self.metrics_key, self.metrics_snapshot, ttl=900, owner=self.name)

    def _compute_metrics(self) -> Dict[str, Any]:
        window_rows = list(self.window)
        if not window_rows:
            return self._empty_metrics()

        prices = [float(row["close"]) for row in window_rows if row.get("close") is not None]
        volumes = [float(row.get("volume") or 0.0) for row in window_rows]
        total_volume = sum(volumes)
        avg_price = sum(prices) / len(prices) if prices else None
        avg_volume = total_volume / len(volumes) if volumes else None
        min_price = min(prices) if prices else None
        max_price = max(prices) if prices else None
        last_price = prices[-1] if prices else None
        first_price = prices[0] if prices else None
        price_return_pct = None
        if first_price and last_price is not None and first_price != 0:
            price_return_pct = ((last_price - first_price) / first_price) * 100.0

        volatility_estimate = None
        if len(prices) >= 2 and avg_price is not None:
            variance = sum((price - avg_price) ** 2 for price in prices) / len(prices)
            volatility_estimate = variance ** 0.5

        vwap = None
        if total_volume > 0:
            vwap = sum(price * volume for price, volume in zip(prices, volumes)) / total_volume

        sma_5 = sum(prices[-5:]) / 5.0 if len(prices) >= 5 else None
        sma_20 = sum(prices[-20:]) / 20.0 if len(prices) >= 20 else None
        trend_score = 0.0
        if sma_5 is not None and sma_20 not in (None, 0):
            trend_score = max(min(((sma_5 - sma_20) / sma_20) * 50.0, 1.0), -1.0)
        elif price_return_pct is not None:
            trend_score = max(min(price_return_pct / 5.0, 1.0), -1.0)

        return {
            "symbol": self.symbol,
            "window_count": len(window_rows),
            "tick_count": self.total_ticks_processed,
            "batch_count": self.total_batches_processed,
            "last_price": last_price,
            "avg_price": avg_price,
            "avg_volume": avg_volume,
            "min_price": min_price,
            "max_price": max_price,
            "price_return_pct": price_return_pct,
            "volatility_estimate": volatility_estimate,
            "vwap": vwap,
            "sma_5": sma_5,
            "sma_20": sma_20,
            "trend_score": trend_score,
            "last_tick_at": self.last_tick_at,
            "mode": self.last_ingest_mode,
        }

    def _update_metrics(self, rows: List[Dict[str, Any]]) -> None:
        self.total_batches_processed += 1
        self.total_ticks_processed += len(rows)
        self.metrics_snapshot = self._compute_metrics()

    def _log_batch_summary(self, rows: List[Dict[str, Any]]) -> None:
        metrics = self.metrics_snapshot
        last_price = metrics.get("last_price")
        avg_price = metrics.get("avg_price")
        return_pct = metrics.get("price_return_pct")
        volatility = metrics.get("volatility_estimate")
        sma_5 = metrics.get("sma_5")
        sma_20 = metrics.get("sma_20")
        logger.info(
            f"[{self.name}] batch={self.total_batches_processed} rows={len(rows)} "
            f"window={metrics.get('window_count')} last={last_price:.2f} avg={avg_price:.2f} "
            f"sma5={sma_5:.2f} sma20={sma_20:.2f} return={return_pct:.3f}% vol={volatility:.4f}"
            if all(value is not None for value in [last_price, avg_price, return_pct, volatility, sma_5, sma_20])
            else f"[{self.name}] batch={self.total_batches_processed} rows={len(rows)} "
                 f"window={metrics.get('window_count')} mode={self.last_ingest_mode}"
        )

    def _risingwave_enabled(self) -> bool:
        return bool(str(config.RISINGWAVE_HOST).strip())

    def _enrich_rows_for_risingwave(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        metrics = self.metrics_snapshot
        enriched = []
        for row in rows:
            enriched.append(
                {
                    "symbol": row.get("symbol", self.symbol),
                    "close": row.get("close"),
                    "volume": row.get("volume"),
                    "timestamp_ms": row.get("timestamp"),
                    "provider": row.get("provider"),
                    "mode": self.last_ingest_mode,
                    "window_count": metrics.get("window_count"),
                    "last_price": metrics.get("last_price"),
                    "avg_price": metrics.get("avg_price"),
                    "avg_volume": metrics.get("avg_volume"),
                    "min_price": metrics.get("min_price"),
                    "max_price": metrics.get("max_price"),
                    "price_return_pct": metrics.get("price_return_pct"),
                    "volatility_estimate": metrics.get("volatility_estimate"),
                    "vwap": metrics.get("vwap"),
                    "sma_5": metrics.get("sma_5"),
                    "sma_20": metrics.get("sma_20"),
                    "trend_score": metrics.get("trend_score"),
                    "last_tick_at": metrics.get("last_tick_at"),
                }
            )
        return enriched

    def _persist_rows(self, rows: List[Dict[str, Any]], *, delta_mode: str) -> None:
        errors: List[str] = []
        try:
            DeltaLakeWriteTask(
                name="Persist Price Stream To Delta",
                uri=self.delta_uri,
                mode=delta_mode,
            ).local(rows)
        except Exception as exc:
            errors.append(f"delta={exc}")

        if self._risingwave_enabled():
            try:
                RisingWaveWriteTask(
                    name="Persist Price Stream To RisingWave",
                    table_name=self.risingwave_table,
                    ddl=_price_risingwave_ddl(),
                ).local(self._enrich_rows_for_risingwave(rows))
            except Exception as exc:
                errors.append(f"risingwave={exc}")

        if errors:
            raise RuntimeError("; ".join(errors))
        self.last_write_count = len(rows)

    def _load_fallback_window(self) -> None:
        fallback_rows = self._fallback_window()
        self.window.clear()
        self.window.extend(fallback_rows)
        self.last_ingest_mode = "fallback"
        self.last_tick_at = time.time()
        self.total_batches_processed += 1
        self.total_ticks_processed += len(fallback_rows)
        self.metrics_snapshot = self._compute_metrics()
        try:
            self._persist_rows(fallback_rows, delta_mode="overwrite")
        except Exception as exc:
            self.last_error = str(exc)
        self._publish_window()
        self._log_batch_summary(fallback_rows)

    def run(self):
        if not self._begin_run_loop():
            return

        source = WebSocketSource(
            url=self.websocket_url,
            batch_size=self.batch_size,
            read_timeout=self.read_timeout,
        )

        try:
            with source.open() as reader:
                for raw_batch in reader.read_batch():
                    if not self.running:
                        break

                    rows = self._normalize_batch(raw_batch)
                    if not rows:
                        continue

                    try:
                        delta_mode = "append"
                        if not self.window:
                            delta_mode = "overwrite"
                        self.window.extend(rows)
                        self.last_ingest_mode = "live_stream"
                        self.last_error = None
                        self.last_tick_at = time.time()
                        self._update_metrics(rows)
                        self._persist_rows(rows, delta_mode=delta_mode)
                        self._publish_window()
                        self._log_batch_summary(rows)
                    except Exception as exc:
                        self.last_error = str(exc)
                        self._publish_window()
                        self._log_batch_summary(rows)
                        logger.warning(f"[{self.name}] Failed to persist live stream batch: {exc}")
        except Exception as exc:
            self.last_error = str(exc)
            logger.warning(f"[{self.name}] WebSocket stream failed. Publishing fallback window: {exc}")
            self._load_fallback_window()
            while self.running:
                time.sleep(1)
        finally:
            self._end_run_loop()

    def get_status(self) -> Dict[str, Any]:
        return {
            "running": self.running,
            "symbol": self.symbol,
            "window_size": len(self.window),
            "requested_window_size": self.window_size,
            "mode": self.last_ingest_mode,
            "context_key": self.context_key,
            "meta_key": self.meta_key,
            "metrics_key": self.metrics_key,
            "delta_uri": self.delta_uri,
            "risingwave_table": _price_risingwave_identifier() if self._risingwave_enabled() else None,
            "last_tick_at": self.last_tick_at,
            "last_write_count": self.last_write_count,
            "last_error": self.last_error,
            "metrics": self.metrics_snapshot,
        }


class ReadLatestPriceWindowTask(BaseTask):
    def run(
        self,
        *,
        context_key: str,
        meta_key: str,
        symbol: str,
        minimum_rows: int = 20,
    ) -> Dict[str, Any]:
        store = get_context()
        rows = store.get(context_key, default=[]) or []
        meta = store.get(meta_key, default={}) or {}
        metrics_key = str(meta.get("metrics_key") or PRICE_METRICS_CONTEXT_KEY)
        metrics = store.get(metrics_key, default={}) or {}

        normalized_rows = [row for row in rows if isinstance(row, dict)]
        normalized_rows.sort(key=lambda row: int(row.get("timestamp") or 0))

        if len(normalized_rows) < minimum_rows:
            raise RuntimeError(
                f"Latest price window for {symbol} has {len(normalized_rows)} rows; need at least {minimum_rows}."
            )

        return {
            "records": normalized_rows,
            "market_state": metrics,
            "mode": str(meta.get("mode") or "unknown"),
            "provider": "websocket_service" if str(meta.get("mode")) == "live_stream" else str(meta.get("mode") or "unknown"),
            "fallback_used": str(meta.get("mode")) == "fallback",
            "table_uri": ohlc_table_uri(),
            "stream_uri": ohlc_stream_uri(),
            "risingwave_table": _price_risingwave_identifier() if str(config.RISINGWAVE_HOST).strip() else None,
            "source_error": meta.get("last_error"),
            "service_meta": meta,
            "metrics": metrics,
        }


def _ensure_price_service(*, symbol: str, window_size: int) -> Dict[str, Any]:
    ensure_ray()
    service_name = PRICE_SERVICE_NAME
    service_cfg = {
        "symbol": symbol,
        "window_size": window_size,
        "context_key": PRICE_WINDOW_CONTEXT_KEY,
        "meta_key": PRICE_META_CONTEXT_KEY,
        "metrics_key": PRICE_METRICS_CONTEXT_KEY,
        "websocket_url": DEFAULT_WEBSOCKET_URL,
        "delta_uri": ohlc_stream_uri(),
        "risingwave_table": PRICE_STREAM_RISINGWAVE_TABLE,
        "batch_size": 20,
        "read_timeout": 1.0,
    }

    try:
        svc = MarketPriceIngestService.connect(service_name)
    except Exception:
        svc = MarketPriceIngestService.deploy(
            name=service_name,
            config=service_cfg,
            num_cpus=0.25,
            max_concurrency=4,
        )
        svc.async_run()
        time.sleep(1)

    status = svc.get_status()
    if not status.get("running"):
        svc.async_run()
        time.sleep(1)
        status = svc.get_status()

    start = time.time()
    while time.time() - start < PRICE_SERVICE_READY_TIMEOUT_SECONDS:
        status = svc.get_status()
        if int(status.get("window_size") or 0) >= min(window_size, 20):
            return status
        time.sleep(1)

    return status


@flow(
    name="Market Pulse Ingest",
    description="Market demo ingestion using Prefect-on-Ray tasks plus a Ray ServiceTask for streaming prices.",
    task_runner=RayTaskRunner(address=config.RAY_ADDRESS),
)
def market_pulse_ingest(
    provider: str = DEFAULT_PROVIDER,
    symbol: str = DEFAULT_SYMBOL,
) -> Dict[str, Any]:
    logger.info(f"=== Starting Market Pulse Ingest Flow for {symbol} (provider={provider}) ===")

    service_status = _ensure_price_service(symbol=symbol, window_size=DEFAULT_PRICE_WINDOW_SIZE)

    news_task = MarketNewsIngestTask()
    read_price_task = ReadLatestPriceWindowTask(name="Read Latest Price Window")

    news_future = news_task.submit(provider=provider, symbol=symbol)
    price_future = read_price_task.submit(
        context_key=PRICE_WINDOW_CONTEXT_KEY,
        meta_key=PRICE_META_CONTEXT_KEY,
        symbol=symbol,
        minimum_rows=20,
    )

    news_result = news_future.result()
    price_result = price_future.result()

    news_write_future = DeltaLakeWriteTask(
        name="Persist Market News",
        uri=news_result["table_uri"],
        mode="overwrite",
    ).submit(news_result["records"])
    price_write_future = DeltaLakeWriteTask(
        name="Persist Latest Price Window",
        uri=price_result["table_uri"],
        mode="overwrite",
    ).submit(price_result["records"])

    news_write_error = None
    price_write_error = None
    try:
        news_write_future.result()
    except Exception as exc:
        news_write_error = str(exc)
        logger.warning(f"[Ingest] Could not persist market news to Delta Lake: {exc}")

    try:
        price_write_future.result()
    except Exception as exc:
        price_write_error = str(exc)
        logger.warning(f"[Ingest] Could not persist latest price window to Delta Lake: {exc}")

    combined = {
        "symbol": symbol,
        "provider_requested": provider,
        "market_state": price_result.get("market_state", {}),
        "news": news_result["records"],
        "ohlc": price_result["records"],
        "news_meta": {
            "mode": news_result["mode"],
            "provider": news_result["provider"],
            "fallback_used": news_result["fallback_used"],
            "persisted": news_write_error is None,
            "table_uri": news_result["table_uri"],
            "persist_error": news_write_error,
            "source_error": news_result["source_error"],
        },
        "ohlc_meta": {
            "mode": price_result["mode"],
            "provider": price_result["provider"],
            "fallback_used": price_result["fallback_used"],
            "persisted": price_write_error is None,
            "table_uri": price_result["table_uri"],
            "stream_uri": price_result["stream_uri"],
            "risingwave_table": price_result["risingwave_table"],
            "persist_error": price_write_error,
            "source_error": price_result["source_error"],
            "service_status": service_status,
            "service_meta": price_result.get("service_meta", {}),
            "metrics": price_result.get("metrics", {}),
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

    logger.info(
        "=== Ingestion Complete === "
        f"news={len(combined['news'])} ({combined['news_meta']['mode']}), "
        f"ohlc={len(combined['ohlc'])} ({combined['ohlc_meta']['mode']})"
    )
    return combined


if __name__ == "__main__":
    market_pulse_ingest()
