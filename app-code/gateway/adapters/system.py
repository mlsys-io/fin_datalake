"""
SystemAdapter: Domain "system"

Handles system-wide observability: querying centralized logs stored in
TimescaleDB, retrieving system health summaries, and reading overseer and audit state.

Supported Actions:
    - logs:             Query system_logs table with filters (component, level, since, agent_name, trace_id, limit)
    - health:           Get current health status of all monitored services
    - overseer_*:       Read overseer snapshots and alerts
    - infra_status:     Probe internal UI targets
    - audit_logs:       Query persisted gateway audit records
    - interface_inventory: Enumerate adapters, routes, and MCP tool surfaces

Required Permissions:
    - all listed actions: system:read
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Any

from gateway.core.adapters import BaseAdapter, ActionNotFoundError
from gateway.core.rbac import Permission
from gateway.models.intent import UserIntent
from gateway.models.user import User
from loguru import logger


class SystemAdapter(BaseAdapter):

    def handles(self) -> str:
        return "system"

    def describe_actions(self) -> list[dict[str, Any]]:
        return [
            {
                "name": "health",
                "description": "Get the current health status of monitored services.",
                "permission": Permission.SYSTEM_READ.value,
                "protocols": ["rest", "mcp"],
            },
            {
                "name": "logs",
                "description": "Query centralized system logs with component, level, agent, and trace filters.",
                "permission": Permission.SYSTEM_READ.value,
                "protocols": ["rest", "mcp"],
            },
            {
                "name": "overseer_snapshots",
                "description": "Read overseer recovery snapshots.",
                "permission": Permission.SYSTEM_READ.value,
                "protocols": ["rest", "mcp"],
            },
            {
                "name": "overseer_alerts",
                "description": "Read overseer recovery alerts.",
                "permission": Permission.SYSTEM_READ.value,
                "protocols": ["rest", "mcp"],
            },
            {
                "name": "infra_status",
                "description": "Probe internal dashboard targets such as Prefect and Ray.",
                "permission": Permission.SYSTEM_READ.value,
                "protocols": ["rest", "mcp"],
            },
            {
                "name": "audit_logs",
                "description": "Query persisted gateway audit records.",
                "permission": Permission.SYSTEM_READ.value,
                "protocols": ["rest", "mcp"],
            },
            {
                "name": "interface_inventory",
                "description": "Enumerate adapters, routes, proxy targets, and MCP tools.",
                "permission": Permission.SYSTEM_READ.value,
                "protocols": ["rest", "mcp"],
            },
        ]

    async def execute(self, user: User, intent: UserIntent) -> Any:
        if intent.action == "logs":
            return self._query_logs(user, intent)
        elif intent.action == "health":
            return await self._get_health_async(user, intent)
        elif intent.action == "overseer_snapshots":
            return await self._get_overseer_snapshots(user, intent)
        elif intent.action == "overseer_alerts":
            return await self._get_overseer_alerts(user, intent)
        elif intent.action == "infra_status":
            return await self._get_infra_status(user, intent)
        elif intent.action == "audit_logs":
            return await self._get_audit_logs(user, intent)
        elif intent.action == "interface_inventory":
            return await self._get_interface_inventory(user, intent)
        
        raise ActionNotFoundError(f"SystemAdapter action '{intent.action}' unknown.")

    async def _get_health_async(self, user: User, intent: UserIntent) -> dict:
        """Get system health summary (async)."""
        self._require_permission(user, Permission.SYSTEM_READ)
        try:
            from gateway.services.system import fetch_overseer_snapshots

            snapshots = await fetch_overseer_snapshots(1)
            if snapshots:
                return self._summarize_health_snapshot(snapshots[-1])

            return {"status": "unknown", "message": "No health snapshots available"}
        except Exception as e:
            logger.warning("Redis health fetch failed: %s", e)
            return {"status": "error", "message": str(e)}

    async def _get_overseer_snapshots(self, user: User, intent: UserIntent) -> dict:
        self._require_permission(user, Permission.SYSTEM_READ)
        from gateway.services.system import fetch_overseer_snapshots

        limit = intent.parameters.get("limit", 50)
        try:
            limit = max(1, min(int(limit), 100))
        except (TypeError, ValueError):
            limit = 50
        snapshots = await fetch_overseer_snapshots(limit)
        return {"snapshots": snapshots, "count": len(snapshots), "limit": limit}

    async def _get_overseer_alerts(self, user: User, intent: UserIntent) -> dict:
        self._require_permission(user, Permission.SYSTEM_READ)
        from gateway.services.system import fetch_overseer_alerts

        limit = intent.parameters.get("limit", 20)
        try:
            limit = max(1, min(int(limit), 100))
        except (TypeError, ValueError):
            limit = 20
        alerts = await fetch_overseer_alerts(limit)
        return {"alerts": alerts, "count": len(alerts), "limit": limit}

    async def _get_infra_status(self, user: User, intent: UserIntent) -> dict:
        self._require_permission(user, Permission.SYSTEM_READ)
        from gateway.services.system import probe_infra_targets

        return await probe_infra_targets()

    async def _get_audit_logs(self, user: User, intent: UserIntent) -> dict:
        self._require_permission(user, Permission.SYSTEM_READ)
        from gateway.services.system import fetch_audit_logs

        params = intent.parameters
        status_code = params.get("status_code")
        if status_code is not None:
            try:
                status_code = int(status_code)
            except (TypeError, ValueError):
                raise ValueError("Parameter 'status_code' must be an integer.")

        limit = params.get("limit", 100)
        try:
            limit = max(1, min(int(limit), 500))
        except (TypeError, ValueError):
            limit = 100

        return await fetch_audit_logs(
            since=params.get("since", "1h"),
            limit=limit,
            request_id=params.get("request_id"),
            source_protocol=params.get("source_protocol"),
            domain=params.get("domain"),
            action=params.get("action"),
            status_code=status_code,
            user_id=params.get("user_id"),
        )

    async def _get_interface_inventory(self, user: User, intent: UserIntent) -> dict:
        self._require_permission(user, Permission.SYSTEM_READ)
        from gateway.services.interfaces import fetch_interface_inventory

        return await fetch_interface_inventory()

    def _query_logs(self, user: User, intent: UserIntent) -> dict:
        """
        Query system_logs from TimescaleDB.

        Parameters:
            component: Filter by component (agent, hub, context, overseer, gateway)
            level: Filter by log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            since: Time window, e.g. "1h", "24h", "7d" (default: "1h")
            agent_name: Filter by specific agent name
            trace_id: Filter by trace ID for request tracing
            limit: Max number of results (default: 100, max: 500)
        """
        self._require_permission(user, Permission.SYSTEM_READ)

        params = intent.parameters
        component = params.get("component")
        level = params.get("level")
        since = params.get("since", "1h")
        agent_name = params.get("agent_name")
        trace_id = params.get("trace_id")
        try:
            limit = max(1, min(int(params.get("limit", 100)), 500))
        except (TypeError, ValueError):
            limit = 100

        # Parse time window
        since_dt = self._parse_since(since)

        # Build query
        conditions = ["time >= %s"]
        values = [since_dt]

        if component:
            conditions.append("component = %s")
            values.append(component)
        if level:
            conditions.append("level = %s")
            values.append(level.upper())
        if agent_name:
            conditions.append("agent_name = %s")
            values.append(agent_name)
        if trace_id:
            conditions.append("trace_id = %s")
            values.append(trace_id)

        where = " AND ".join(conditions)
        sql = (
            f"SELECT time, component, level, message, trace_id, agent_name, metadata "
            f"FROM system_logs WHERE {where} "
            f"ORDER BY time DESC LIMIT %s"
        )
        values.append(limit)

        try:
            rows = self._execute_query(sql, values)
            available = True
            error = None
        except Exception as exc:
            logger.warning(f"System log query unavailable: {exc}")
            rows = []
            available = False
            error = str(exc)

        return {
            "logs": [
                {
                    "time": row[0].isoformat() if row[0] else None,
                    "component": row[1],
                    "level": row[2],
                    "message": row[3],
                    "trace_id": row[4],
                    "agent_name": row[5],
                    "metadata": row[6],
                }
                for row in rows
            ],
            "count": len(rows),
            "available": available,
            "error": error,
            "query": {
                "component": component,
                "level": level,
                "since": since,
                "agent_name": agent_name,
                "trace_id": trace_id,
                "limit": limit,
            },
        }

    # NOTE: Removed redundant sync _get_health(); all callers now use _get_health_async.

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _summarize_health_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
        summary = {}
        all_healthy = True
        for name, metrics in snapshot.get("services", {}).items():
            if not isinstance(metrics, dict):
                summary[name] = {"healthy": False, "error": "Invalid service metrics"}
                all_healthy = False
                continue

            healthy = bool(metrics.get("healthy", False))
            summary[name] = {
                "healthy": healthy,
                "error": metrics.get("error", None),
            }
            if not healthy:
                all_healthy = False

        return {
            "source": "redis (overseer)",
            "status": "healthy" if all_healthy else "degraded",
            "components": summary,
            "timestamp": snapshot.get("timestamp"),
        }

    @staticmethod
    def _parse_since(since: str) -> datetime:
        """Parse a time window string like '1h', '24h', '7d' into a datetime."""
        now = datetime.now(timezone.utc)
        unit = since[-1].lower()
        try:
            value = int(since[:-1])
        except (ValueError, IndexError):
            value = 1
            unit = "h"

        if unit == "m":
            return now - timedelta(minutes=value)
        elif unit == "h":
            return now - timedelta(hours=value)
        elif unit == "d":
            return now - timedelta(days=value)
        else:
            return now - timedelta(hours=1)

    @staticmethod
    def _execute_query(sql: str, params: list) -> list:
        """Execute a read query against TimescaleDB."""
        try:
            import psycopg2
        except ImportError:
            raise RuntimeError("psycopg2 is required for system log queries")

        host = os.environ.get("TSDB_HOST", "localhost")
        port = os.environ.get("TSDB_PORT", "5432")
        user = os.environ.get("TSDB_USER", "app")
        password = os.environ.get("TSDB_PASSWORD", "")
        database = os.environ.get("TSDB_DATABASE", "app")

        conn = psycopg2.connect(
            host=host, port=port, user=user,
            password=password, dbname=database,
        )
        try:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            cursor.close()
            return rows
        finally:
            conn.close()
