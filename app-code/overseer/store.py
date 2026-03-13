"""
MetricsStore — Rolling-window in-memory store for system snapshots.

Keeps the last N snapshots so the Gateway can serve time-series data
to the React dashboard without needing an external database.
"""

from __future__ import annotations

import json
import os
from typing import Optional
import redis.asyncio as redis

from overseer.models import OverseerAction, SystemSnapshot, ServiceMetrics


class MetricsStore:
    """
    Redis-backed rolling window of SystemSnapshots and OverseerActions.

    Used by:
      - The Overseer loop (write)
      - The Gateway 'system' adapter (read)
      
    This decoupling allows Gateway and Overseer to run in separate pods.
    """

    def __init__(self, max_snapshots: int = 200, max_alerts: int = 100):
        self.max_snapshots = max_snapshots
        self.max_alerts = max_alerts
        redis_url = os.getenv("OVERSEER_REDIS_URL", "redis://:redis-lakehouse-pass@localhost:6379/0")
        self.redis = redis.from_url(redis_url, decode_responses=True)
        self._snapshot_key = "overseer:snapshots"
        self._alert_key = "overseer:alerts"

    async def append_snapshot(self, snapshot: SystemSnapshot) -> None:
        """Serialize the snapshot and push to the Redis list, keeping only the latest N."""
        import dataclasses
        data = json.dumps(dataclasses.asdict(snapshot))
        
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.lpush(self._snapshot_key, data)
            pipe.ltrim(self._snapshot_key, 0, self.max_snapshots - 1)
            await pipe.execute()

    async def append_alert(self, action: OverseerAction) -> None:
        """Push a serialized alert to Redis."""
        data = json.dumps(action.to_alert())
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.lpush(self._alert_key, data)
            pipe.ltrim(self._alert_key, 0, self.max_alerts - 1)
            await pipe.execute()

    async def latest(self) -> Optional[SystemSnapshot]:
        """Fetch the most recent snapshot."""
        data = await self.redis.lindex(self._snapshot_key, 0)
        if data:
            raw = json.loads(data)
            if "services" in raw:
                raw["services"] = {k: ServiceMetrics(**v) for k, v in raw["services"].items()}
            return SystemSnapshot(**raw)
        return None

    async def recent_alerts(self, n: int = 20) -> list[dict]:
        """Fetch the recent alerts."""
        items = await self.redis.lrange(self._alert_key, 0, n - 1)
        return [json.loads(item) for item in items]

    async def history(self, n: int = 50) -> list[dict]:
        """Return the last N snapshots as serializable dicts for the dashboard."""
        items = await self.redis.lrange(self._snapshot_key, 0, n - 1)
        result = []
        # Redis lrange returns newest first, we want chronological (oldest first)
        for data in reversed(items):
            raw = json.loads(data)
            if "services" in raw:
                raw["services"] = {k: ServiceMetrics(**v) for k, v in raw["services"].items()}
            snap = SystemSnapshot(**raw)
            entry = {"timestamp": snap.timestamp, "services": {}}
            for svc_name, metrics in snap.services.items():
                entry["services"][svc_name] = {
                    "healthy": metrics.healthy,
                    "data": metrics.data,
                    "error": metrics.error,
                }
            result.append(entry)
        return result

    async def get_health_summary(self) -> dict:
        """Quick health overview for the dashboard header cards."""
        snap = await self.latest()
        if not snap:
            return {"status": "unknown", "services": {}}
        summary = {}
        all_healthy = True
        for name, metrics in snap.services.items():
            summary[name] = {
                "healthy": metrics.healthy,
                "error": metrics.error,
            }
            if not metrics.healthy:
                all_healthy = False
        return {
            "status": "healthy" if all_healthy else "degraded",
            "services": summary,
            "timestamp": snap.timestamp,
        }
