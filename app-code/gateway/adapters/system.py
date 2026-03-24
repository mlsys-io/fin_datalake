"""
SystemAdapter: Domain "system"

Handles system-wide observability: querying centralized logs stored in
TimescaleDB and retrieving system health summaries.

Supported Actions:
    - logs:    Query system_logs table with filters (component, level, since, limit)
    - health:  Get current health status of all monitored services

Required Permissions:
    - logs / health: system:read
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

    async def execute(self, user: User, intent: UserIntent) -> Any:
        self._require_permission(user, Permission.SYSTEM_ADMIN)

        if intent.action == "logs":
            return self._query_logs(user, intent)
        elif intent.action == "health":
            return await self._get_health_async(user, intent)
        
        raise ActionNotFoundError(f"SystemAdapter action '{intent.action}' unknown.")

    async def _get_health_async(self, user: User, intent: UserIntent) -> dict:
        """Get system health summary (async)."""
        self._require_permission(user, Permission.SYSTEM_READ)
        import json

        # Try to get health from Redis (written by Overseer MetricsStore)
        try:
            from gateway.core.redis import get_redis_client
            r = get_redis_client()
            if not r:
                return {"status": "unknown", "message": "Redis not configured"}

            snapshot_data = await r.lindex("overseer:snapshots", 0)
            await r.aclose()

            if snapshot_data:
                snap = json.loads(snapshot_data)
                summary = {}
                all_healthy = True
                for name, metrics in snap.get("services", {}).items():
                    summary[name] = {
                        "healthy": metrics.get("healthy", False),
                        "error": metrics.get("error", None)
                    }
                    if not metrics.get("healthy", False):
                        all_healthy = False

                return {
                    "source": "redis (overseer)",
                    "status": "healthy" if all_healthy else "degraded",
                    "components": summary,
                    "timestamp": snap.get("timestamp")
                }
            return {"status": "unknown", "message": "No health snapshots available"}
        except Exception as e:
            logger.warning("Redis health fetch failed: %s", e)
            return {"status": "error", "message": str(e)}

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
        limit = min(int(params.get("limit", 100)), 500)

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

        rows = self._execute_query(sql, values)
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
            "query": {
                "component": component,
                "level": level,
                "since": since,
                "limit": limit,
            },
        }

    # NOTE: Removed redundant sync _get_health(); all callers now use _get_health_async.

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

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
