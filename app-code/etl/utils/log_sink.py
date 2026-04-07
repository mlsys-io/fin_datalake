"""
TimescaleDB Log Sink — Persistent structured logging.

A custom loguru sink that batches log records and writes them to a
``system_logs`` hypertable in TimescaleDB. Designed for Kubernetes
environments where pods are ephemeral and local filesystem is unreliable.

Features:
- Batched inserts (configurable batch size + flush interval)
- Auto-creates the ``system_logs`` table on first connection
- Graceful degradation: if TimescaleDB is unreachable, logs a warning
  and continues — the application never crashes due to logging failure
- Thread-safe buffer with lock

Usage::

    from etl.utils.log_sink import TimescaleLogSink

    sink = TimescaleLogSink(component="agent")
    logger.add(sink, level="INFO", serialize=False)
"""

from __future__ import annotations

import threading
import time
import atexit
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS system_logs (
    time         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    component    TEXT NOT NULL,
    level        TEXT NOT NULL,
    message      TEXT NOT NULL,
    trace_id     TEXT,
    agent_name   TEXT,
    metadata     JSONB DEFAULT '{}'
);
"""

_CREATE_HYPERTABLE_SQL = """
SELECT create_hypertable('system_logs', 'time', if_not_exists => TRUE);
"""

_ADD_RETENTION_SQL = """
SELECT add_retention_policy('system_logs', INTERVAL '30 days', if_not_exists => TRUE);
"""

_CREATE_INDEXES_SQL = """
CREATE INDEX IF NOT EXISTS idx_logs_component ON system_logs (component, time DESC);
CREATE INDEX IF NOT EXISTS idx_logs_level     ON system_logs (level, time DESC);
CREATE INDEX IF NOT EXISTS idx_logs_trace     ON system_logs (trace_id, time DESC);
"""

_INSERT_SQL = """
INSERT INTO system_logs (time, component, level, message, trace_id, agent_name, metadata)
VALUES %s
"""


# ---------------------------------------------------------------------------
# DSN Builder
# ---------------------------------------------------------------------------

def build_dsn() -> Optional[str]:
    """Build a PostgreSQL DSN from central config when Timescale is configured."""
    from etl.config import config

    host = str(config.TSDB_HOST or "").strip()
    if not host:
        return None

    port = str(config.TSDB_PORT)
    user = str(config.TSDB_USER)
    password = str(config.TSDB_PASSWORD)
    database = str(config.TSDB_DATABASE)
    return f"host={host} port={port} user={user} password={password} dbname={database}"


def is_timescale_logging_configured() -> bool:
    """Return True when Timescale logging has enough config to connect."""
    return build_dsn() is not None


# ---------------------------------------------------------------------------
# Sink
# ---------------------------------------------------------------------------

class TimescaleLogSink:
    """
    Custom loguru sink that writes structured log records to TimescaleDB.

    Records are buffered in memory and flushed to the database either when
    the buffer reaches ``batch_size`` or every ``flush_interval`` seconds
    (whichever comes first).

    If the database is unreachable, the sink prints a warning to stderr
    and discards the batch — the application is never affected.

    Args:
        component: Identifies the source component (e.g. 'agent', 'hub')
        dsn: PostgreSQL connection string. If None, built from env vars.
        batch_size: Number of records to buffer before flushing
        flush_interval: Seconds between automatic flushes
    """

    def __init__(
        self,
        component: str,
        dsn: Optional[str] = None,
        batch_size: int = 50,
        flush_interval: float = 5.0,
    ):
        self.component = component
        self.dsn = dsn or build_dsn()
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._disabled = self.dsn is None

        self._buffer: List[tuple] = []
        self._lock = threading.Lock()
        self._conn = None
        self._initialized = False
        self._failed = False  # True if we failed to connect; avoids log spam

        # Background flush thread
        self._stop_event = threading.Event()
        self._flush_thread = threading.Thread(
            target=self._periodic_flush, daemon=True, name="log-sink-flush"
        )
        self._flush_thread.start()

        # Flush on interpreter exit
        atexit.register(self._shutdown)

    # ------------------------------------------------------------------
    # Public interface (called by loguru)
    # ------------------------------------------------------------------

    def write(self, message) -> None:
        """
        Called by loguru for each log record.

        ``message`` is a loguru ``Message`` object with a ``.record`` attribute
        containing structured fields (level, time, message text, extra, etc.).
        """
        record = message.record
        extra = record.get("extra", {})

        row = (
            record["time"].astimezone(timezone.utc),
            self.component,
            record["level"].name,
            str(record["message"]),
            extra.get("trace_id"),
            extra.get("agent_name"),
            self._safe_json(extra),
        )

        with self._lock:
            self._buffer.append(row)
            if len(self._buffer) >= self.batch_size:
                self._flush_locked()

    # ------------------------------------------------------------------
    # Flush logic
    # ------------------------------------------------------------------

    def flush(self) -> None:
        """Manually flush buffered records to the database."""
        with self._lock:
            self._flush_locked()

    def _flush_locked(self) -> None:
        """Flush while holding the lock. Must be called under self._lock."""
        if not self._buffer:
            return

        batch = list(self._buffer)
        self._buffer.clear()

        try:
            self._ensure_connection()
            if self._conn is None:
                return  # Connection failed; batch is lost (graceful degradation)

            from psycopg2.extras import execute_values

            cursor = self._conn.cursor()
            execute_values(cursor, _INSERT_SQL, batch)
            self._conn.commit()
            cursor.close()

        except Exception as e:
            # Don't crash the application for a logging failure
            self._close_connection()
            if not self._failed:
                import sys
                print(
                    f"[TimescaleLogSink] Failed to write logs: {e}",
                    file=sys.stderr,
                )
                self._failed = True

    def _periodic_flush(self) -> None:
        """Background thread: flush every ``flush_interval`` seconds."""
        while not self._stop_event.is_set():
            self._stop_event.wait(self.flush_interval)
            with self._lock:
                self._flush_locked()

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def _ensure_connection(self) -> None:
        """Establish a connection and create the table if needed."""
        if self._disabled:
            return

        if self._conn is not None:
            return

        try:
            import psycopg2
        except ImportError:
            if not self._failed:
                import sys
                print(
                    "[TimescaleLogSink] psycopg2 not installed — DB logging disabled",
                    file=sys.stderr,
                )
                self._failed = True
            return

        try:
            self._conn = psycopg2.connect(self.dsn)
            self._conn.autocommit = False

            if not self._initialized:
                self._init_table()
                self._initialized = True

            self._failed = False  # Connection recovered

        except Exception as e:
            self._conn = None
            if not self._failed:
                import sys
                print(
                    f"[TimescaleLogSink] Cannot connect to TimescaleDB: {e}",
                    file=sys.stderr,
                )
                self._failed = True

    def _init_table(self) -> None:
        """Create the system_logs table and hypertable if they don't exist."""
        cursor = self._conn.cursor()
        try:
            cursor.execute(_CREATE_TABLE_SQL)
            self._conn.commit()

            # create_hypertable may fail if extension not available
            try:
                cursor.execute(_CREATE_HYPERTABLE_SQL)
                self._conn.commit()
            except Exception:
                self._conn.rollback()  # Not a hypertable — still works as plain table

            try:
                cursor.execute(_ADD_RETENTION_SQL)
                self._conn.commit()
            except Exception:
                self._conn.rollback()  # Retention requires TimescaleDB extension

            cursor.execute(_CREATE_INDEXES_SQL)
            self._conn.commit()
        finally:
            cursor.close()

    def _close_connection(self) -> None:
        """Close the database connection."""
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _safe_json(extra: dict) -> str:
        """Convert extra dict to JSON string, filtering out non-serializable values."""
        import json

        # Filter out loguru internal keys and non-serializable values
        skip_keys = {"trace_id", "agent_name"}
        safe = {}
        for k, v in extra.items():
            if k in skip_keys:
                continue
            try:
                json.dumps(v)
                safe[k] = v
            except (TypeError, ValueError):
                safe[k] = str(v)
        return json.dumps(safe) if safe else "{}"

    def _shutdown(self) -> None:
        """Flush remaining records and close connection on exit."""
        self._stop_event.set()
        self.flush()
        self._close_connection()
