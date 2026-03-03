"""
Tests for MetricsStore: rolling window, alerts, and health summary.
"""

import pytest
from overseer.models import (
    ActionType,
    OverseerAction,
    ServiceMetrics,
    SystemSnapshot,
)
from overseer.store import MetricsStore


class TestMetricsStore:
    def setup_method(self):
        self.store = MetricsStore(max_snapshots=5, max_alerts=3)

    def test_empty_latest_returns_none(self):
        assert self.store.latest() is None

    def test_append_and_latest(self):
        snap = SystemSnapshot()
        snap.services["ray"] = ServiceMetrics(service="ray", healthy=True)
        self.store.append_snapshot(snap)
        assert self.store.latest() is snap

    def test_rolling_window_evicts_oldest(self):
        for i in range(10):
            snap = SystemSnapshot()
            snap.services["test"] = ServiceMetrics(service="test", healthy=True, data={"i": i})
            self.store.append_snapshot(snap)
        # max_snapshots=5, so oldest should be evicted
        history = self.store.history(n=100)
        assert len(history) == 5
        # Most recent should have i=9
        assert history[-1]["services"]["test"]["data"]["i"] == 9

    def test_recent_alerts(self):
        for i in range(5):
            action = OverseerAction(type=ActionType.ALERT, reason=f"alert-{i}")
            self.store.append_alert(action)
        alerts = self.store.recent_alerts(n=10)
        # max_alerts=3, so only the last 3 should remain
        assert len(alerts) == 3
        assert alerts[-1]["reason"] == "alert-4"

    def test_get_health_summary_when_empty(self):
        summary = self.store.get_health_summary()
        assert summary["status"] == "unknown"

    def test_get_health_summary_all_healthy(self):
        snap = SystemSnapshot()
        snap.services["ray"] = ServiceMetrics(service="ray", healthy=True)
        snap.services["kafka"] = ServiceMetrics(service="kafka", healthy=True)
        self.store.append_snapshot(snap)
        summary = self.store.get_health_summary()
        assert summary["status"] == "healthy"

    def test_get_health_summary_degraded(self):
        snap = SystemSnapshot()
        snap.services["ray"] = ServiceMetrics(service="ray", healthy=True)
        snap.services["kafka"] = ServiceMetrics(service="kafka", healthy=False, error="down")
        self.store.append_snapshot(snap)
        summary = self.store.get_health_summary()
        assert summary["status"] == "degraded"
        assert summary["services"]["kafka"]["error"] == "down"

    def test_history_returns_serializable(self):
        snap = SystemSnapshot()
        snap.services["test"] = ServiceMetrics(service="test", healthy=True, data={"k": "v"})
        self.store.append_snapshot(snap)
        history = self.store.history()
        # Should be a list of plain dicts (JSON-serializable)
        assert isinstance(history, list)
        assert isinstance(history[0], dict)
        assert "timestamp" in history[0]
        assert history[0]["services"]["test"]["data"]["k"] == "v"
