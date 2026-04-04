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


_UNSET = object()


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
ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS managed_by_overseer BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS desired_status TEXT NOT NULL DEFAULT 'running';
ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS observed_status TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS health_status TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS recovery_state TEXT NOT NULL DEFAULT 'idle';
ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS last_reconciled_at TIMESTAMPTZ;
ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS last_failure_reason TEXT;
ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS last_action_type TEXT;
ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS reconcile_notes TEXT;
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
    managed_by_overseer,
    desired_status,
    observed_status,
    health_status,
    recovery_state,
    last_reconciled_at,
    last_failure_reason,
    last_action_type,
    reconcile_notes,
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
    %(managed_by_overseer)s,
    %(desired_status)s,
    %(observed_status)s,
    %(health_status)s,
    %(recovery_state)s,
    %(last_reconciled_at)s,
    %(last_failure_reason)s,
    %(last_action_type)s,
    %(reconcile_notes)s,
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
    managed_by_overseer = EXCLUDED.managed_by_overseer,
    desired_status = EXCLUDED.desired_status,
    observed_status = EXCLUDED.observed_status,
    health_status = EXCLUDED.health_status,
    recovery_state = EXCLUDED.recovery_state,
    last_reconciled_at = EXCLUDED.last_reconciled_at,
    last_failure_reason = EXCLUDED.last_failure_reason,
    last_action_type = EXCLUDED.last_action_type,
    reconcile_notes = EXCLUDED.reconcile_notes,
    runtime_source = EXCLUDED.runtime_source,
    runtime_namespace = EXCLUDED.runtime_namespace,
    route_prefix = EXCLUDED.route_prefix,
    updated_at = EXCLUDED.updated_at;
"""

_UPDATE_STATUS_SQL = """
UPDATE agent_definitions
SET
    status = %(status)s,
    observed_status = COALESCE(%(observed_status)s, observed_status),
    health_status = COALESCE(%(health_status)s, health_status),
    recovery_state = COALESCE(%(recovery_state)s, recovery_state),
    last_reconciled_at = COALESCE(%(last_reconciled_at)s, last_reconciled_at),
    last_failure_reason = CASE
        WHEN %(set_last_failure_reason)s THEN %(last_failure_reason)s
        ELSE last_failure_reason
    END,
    last_action_type = CASE
        WHEN %(set_last_action_type)s THEN %(last_action_type)s
        ELSE last_action_type
    END,
    reconcile_notes = CASE
        WHEN %(set_reconcile_notes)s THEN %(reconcile_notes)s
        ELSE reconcile_notes
    END,
    last_seen_at = COALESCE(%(last_seen_at)s, last_seen_at),
    last_heartbeat_at = COALESCE(%(last_heartbeat_at)s, last_heartbeat_at),
    updated_at = %(updated_at)s
WHERE name = %(name)s;
"""

_LIST_AGENTS_SQL = """
SELECT
    name,
    capabilities,
    capability_specs,
    metadata,
    deployment_metadata,
    registered_at,
    last_seen_at,
    last_heartbeat_at,
    status,
    managed_by_overseer,
    desired_status,
    observed_status,
    health_status,
    recovery_state,
    last_reconciled_at,
    last_failure_reason,
    last_action_type,
    reconcile_notes,
    runtime_source,
    runtime_namespace,
    route_prefix,
    is_enabled
FROM agent_definitions
WHERE (%(runtime_source)s IS NULL OR runtime_source = %(runtime_source)s)
  AND (%(enabled_only)s = FALSE OR is_enabled = TRUE)
ORDER BY name ASC;
"""

_DELETE_AGENT_SQL = """
DELETE FROM agent_definitions
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


def _parse_json_text(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    try:
        return json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def _row_to_agent_dict(row: tuple[Any, ...]) -> dict[str, Any]:
    desired_status = row[10]
    observed_status = row[11]
    health_status = row[12]
    return {
        "name": row[0],
        "capabilities": _parse_json_text(row[1], []),
        "capability_specs": _parse_json_text(row[2], []),
        "metadata": _parse_json_text(row[3], {}),
        "deployment_metadata": _parse_json_text(row[4], {}),
        "registered_at": row[5].isoformat() if row[5] else None,
        "last_seen_at": row[6].isoformat() if row[6] else None,
        "last_heartbeat_at": row[7].isoformat() if row[7] else None,
        "status": row[8],
        "managed_by_overseer": bool(row[9]),
        "desired_status": row[10],
        "observed_status": row[11],
        "health_status": row[12],
        "recovery_state": row[13],
        "last_reconciled_at": row[14].isoformat() if row[14] else None,
        "last_failure_reason": row[15],
        "last_action_type": row[16],
        "reconcile_notes": row[17],
        "runtime_source": row[18],
        "runtime_namespace": row[19],
        "route_prefix": row[20],
        "is_enabled": bool(row[21]),
        "alive": (
            desired_status == "running"
            and observed_status in {"ready", "degraded", "recovering"}
            and health_status != "offline"
        ),
        "source": "catalog",
    }


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
    managed_by_overseer: bool = True,
    desired_status: str = "running",
    observed_status: str = "ready",
    health_status: str = "healthy",
    recovery_state: str = "idle",
    last_failure_reason: str | None = None,
    last_action_type: str | None = None,
    reconcile_notes: str | None = None,
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
        "managed_by_overseer": managed_by_overseer,
        "desired_status": desired_status,
        "observed_status": observed_status,
        "health_status": health_status,
        "recovery_state": recovery_state,
        "last_reconciled_at": now,
        "last_failure_reason": last_failure_reason,
        "last_action_type": last_action_type,
        "reconcile_notes": reconcile_notes,
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
    observed_status: str | None = None,
    health_status: str | None = None,
    recovery_state: str | None = None,
    last_failure_reason: str | None | object = _UNSET,
    last_action_type: str | None | object = _UNSET,
    reconcile_notes: str | None | object = _UNSET,
    reconciled: bool = False,
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
        "observed_status": observed_status,
        "health_status": health_status,
        "recovery_state": recovery_state,
        "last_reconciled_at": now if reconciled else None,
        "last_failure_reason": None if last_failure_reason is _UNSET else last_failure_reason,
        "set_last_failure_reason": last_failure_reason is not _UNSET,
        "last_action_type": None if last_action_type is _UNSET else last_action_type,
        "set_last_action_type": last_action_type is not _UNSET,
        "reconcile_notes": None if reconcile_notes is _UNSET else reconcile_notes,
        "set_reconcile_notes": reconcile_notes is not _UNSET,
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


def list_agent_catalog_entries(
    *,
    runtime_source: str | None = None,
    enabled_only: bool = True,
) -> list[dict[str, Any]]:
    """
    Read durable catalog entries directly from Timescale/Postgres.
    """
    dsn = _get_dsn()
    if not dsn:
        return []

    import psycopg2

    conn = psycopg2.connect(dsn)
    try:
        with conn:
            with conn.cursor() as cursor:
                _ensure_schema(cursor)
                cursor.execute(
                    _LIST_AGENTS_SQL,
                    {
                        "runtime_source": runtime_source,
                        "enabled_only": enabled_only,
                    },
                )
                rows = cursor.fetchall()
        return [_row_to_agent_dict(row) for row in rows]
    finally:
        conn.close()


def delete_agent_catalog_entry(name: str) -> bool:
    """
    Delete a durable catalog entry by exact deployment name.
    """
    dsn = _get_dsn()
    if not dsn:
        return False

    import psycopg2

    conn = psycopg2.connect(dsn)
    try:
        with conn:
            with conn.cursor() as cursor:
                _ensure_schema(cursor)
                cursor.execute(_DELETE_AGENT_SQL, {"name": name})
                return cursor.rowcount > 0
    finally:
        conn.close()
