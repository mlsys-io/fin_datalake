"""
Durable agent catalog helpers.

These helpers let Ray-hosted agents write directly to the shared gateway
catalog table without depending on gateway-only async DB libraries.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any


_CREATE_AGENT_DEFINITIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS agent_definitions (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    capabilities TEXT NOT NULL DEFAULT '[]',
    capability_specs TEXT NOT NULL DEFAULT '[]',
    metadata TEXT NOT NULL DEFAULT '{}',
    registered_at TIMESTAMPTZ NULL,
    last_seen_at TIMESTAMPTZ NULL,
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

_ENSURE_AGENT_DEFINITIONS_COLUMNS_SQL = """
ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS last_heartbeat_at TIMESTAMPTZ;
ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS runtime_source TEXT;
ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS runtime_namespace TEXT;
ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS route_prefix TEXT;
ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS deployment_metadata TEXT NOT NULL DEFAULT '{}';
"""

_UPSERT_AGENT_SQL = """
INSERT INTO agent_definitions (
    id,
    name,
    capabilities,
    capability_specs,
    metadata,
    deployment_metadata,
    registered_at,
    last_seen_at,
    last_heartbeat_at,
    status,
    runtime_source,
    runtime_namespace,
    route_prefix,
    is_enabled,
    created_at,
    updated_at
)
VALUES (
    %(id)s,
    %(name)s,
    %(capabilities)s,
    %(capability_specs)s,
    %(metadata)s,
    %(deployment_metadata)s,
    %(registered_at)s,
    %(last_seen_at)s,
    %(last_heartbeat_at)s,
    %(status)s,
    %(runtime_source)s,
    %(runtime_namespace)s,
    %(route_prefix)s,
    TRUE,
    %(created_at)s,
    %(updated_at)s
)
ON CONFLICT (name) DO UPDATE SET
    capabilities = EXCLUDED.capabilities,
    capability_specs = EXCLUDED.capability_specs,
    metadata = EXCLUDED.metadata,
    deployment_metadata = EXCLUDED.deployment_metadata,
    registered_at = COALESCE(EXCLUDED.registered_at, agent_definitions.registered_at),
    last_seen_at = EXCLUDED.last_seen_at,
    last_heartbeat_at = EXCLUDED.last_heartbeat_at,
    status = EXCLUDED.status,
    runtime_source = EXCLUDED.runtime_source,
    runtime_namespace = EXCLUDED.runtime_namespace,
    route_prefix = EXCLUDED.route_prefix,
    updated_at = EXCLUDED.updated_at;
"""

_UPDATE_STATUS_SQL = """
UPDATE agent_definitions
SET
    status = %(status)s,
    last_seen_at = COALESCE(%(last_seen_at)s, last_seen_at),
    last_heartbeat_at = COALESCE(%(last_heartbeat_at)s, last_heartbeat_at),
    updated_at = %(updated_at)s
WHERE name = %(name)s;
"""


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _get_dsn() -> str | None:
    from etl.config import config

    host = str(config.TSDB_HOST or "").strip()
    if not host:
        return None

    return (
        f"host={host} port={config.TSDB_PORT} "
        f"user={config.TSDB_USER} password={config.TSDB_PASSWORD} "
        f"dbname={config.TSDB_DATABASE}"
    )


def _ensure_schema(cursor) -> None:
    cursor.execute(_CREATE_AGENT_DEFINITIONS_TABLE_SQL)
    cursor.execute(_ENSURE_AGENT_DEFINITIONS_COLUMNS_SQL)


def upsert_agent_catalog_entry(
    *,
    name: str,
    capabilities: list[Any],
    capability_specs: list[dict[str, Any]],
    metadata: dict[str, Any] | None = None,
    deployment_metadata: dict[str, Any] | None = None,
    route_prefix: str | None = None,
    runtime_namespace: str | None = None,
    runtime_source: str = "ray-serve",
    status: str = "alive",
    registered_at: datetime | None = None,
) -> bool:
    """
    Insert or update a durable catalog record for a live agent.
    """
    dsn = _get_dsn()
    if not dsn:
        return False

    import psycopg2

    now = _now()
    payload = {
        "id": str(uuid.uuid4()),
        "name": name,
        "capabilities": json.dumps(capabilities or []),
        "capability_specs": json.dumps(capability_specs or []),
        "metadata": json.dumps(metadata or {}),
        "deployment_metadata": json.dumps(deployment_metadata or {}),
        "registered_at": registered_at or now,
        "last_seen_at": now,
        "last_heartbeat_at": now,
        "status": status,
        "runtime_source": runtime_source,
        "runtime_namespace": runtime_namespace,
        "route_prefix": route_prefix,
        "created_at": now,
        "updated_at": now,
    }

    conn = psycopg2.connect(dsn)
    try:
        with conn:
            with conn.cursor() as cursor:
                _ensure_schema(cursor)
                cursor.execute(_UPSERT_AGENT_SQL, payload)
        return True
    finally:
        conn.close()


def update_agent_catalog_status(
    *,
    name: str,
    status: str,
    mark_seen: bool = False,
    heartbeat: bool = False,
) -> bool:
    """
    Update durable runtime status for an already-registered agent.
    """
    dsn = _get_dsn()
    if not dsn:
        return False

    import psycopg2

    now = _now()
    payload = {
        "name": name,
        "status": status,
        "last_seen_at": now if mark_seen else None,
        "last_heartbeat_at": now if heartbeat else None,
        "updated_at": now,
    }

    conn = psycopg2.connect(dsn)
    try:
        with conn:
            with conn.cursor() as cursor:
                _ensure_schema(cursor)
                cursor.execute(_UPDATE_STATUS_SQL, payload)
        return True
    finally:
        conn.close()
