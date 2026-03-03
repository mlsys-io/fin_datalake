"""
MetricsStore — Rolling-window in-memory store for system snapshots.

Keeps the last N snapshots so the Gateway can serve time-series data
to the React dashboard without needing an external database.
"""

from __future__ import annotations

import threading
from collections import deque
from typing import Optional

from overseer.models import OverseerAction, SystemSnapshot


class MetricsStore:
    """
    Thread-safe rolling window of SystemSnapshots and OverseerActions.

    Used by:
      - The Overseer loop (write)
      - The Gateway 'system' adapter (read)
    """

    def __init__(self, max_snapshots: int = 200, max_alerts: int = 100):
        self._snapshots: deque[SystemSnapshot] = deque(maxlen=max_snapshots)
        self._alerts: deque[dict] = deque(maxlen=max_alerts)
        self._lock = threading.Lock()

    def append_snapshot(self, snapshot: SystemSnapshot) -> None:
        with self._lock:
            self._snapshots.append(snapshot)

    def append_alert(self, action: OverseerAction) -> None:
        with self._lock:
            self._alerts.append(action.to_alert())

    def latest(self) -> Optional[SystemSnapshot]:
        with self._lock:
            return self._snapshots[-1] if self._snapshots else None

    def recent_alerts(self, n: int = 20) -> list[dict]:
        with self._lock:
            return list(self._alerts)[-n:]

    def history(self, n: int = 50) -> list[dict]:
        """Return the last N snapshots as serializable dicts for the dashboard."""
        with self._lock:
            snapshots = list(self._snapshots)[-n:]
        result = []
        for snap in snapshots:
            entry = {"timestamp": snap.timestamp, "services": {}}
            for svc_name, metrics in snap.services.items():
                entry["services"][svc_name] = {
                    "healthy": metrics.healthy,
                    "data": metrics.data,
                    "error": metrics.error,
                }
            result.append(entry)
        return result

    def get_health_summary(self) -> dict:
        """Quick health overview for the dashboard header cards."""
        snap = self.latest()
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
