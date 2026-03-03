"""
Tests for Overseer data models: ServiceEndpoint, OverseerAction, ActionResult.
"""

import pytest
from overseer.models import (
    ActionResult,
    ActionType,
    OverseerAction,
    ServiceEndpoint,
    ServiceMetrics,
    SystemSnapshot,
)


class TestServiceEndpoint:
    def test_base_url_http(self):
        ep = ServiceEndpoint(name="ray", host="myhost", port=8265, protocol="http")
        assert ep.base_url == "http://myhost:8265"

    def test_base_url_defaults(self):
        ep = ServiceEndpoint(name="test")
        assert ep.base_url == "http://localhost:8080"

    def test_extra_defaults_to_empty(self):
        ep = ServiceEndpoint(name="test")
        assert ep.extra == {}


class TestOverseerAction:
    def test_to_alert_contains_all_fields(self):
        action = OverseerAction(
            type=ActionType.SCALE_UP,
            agent="SentimentAgent",
            count=3,
            reason="Lag too high",
        )
        alert = action.to_alert()
        assert alert["type"] == "scale_up"
        assert alert["agent"] == "SentimentAgent"
        assert alert["count"] == 3
        assert alert["reason"] == "Lag too high"
        assert "timestamp" in alert

    def test_action_type_enum(self):
        assert ActionType.SCALE_UP.value == "scale_up"
        assert ActionType.RESPAWN.value == "respawn"


class TestSystemSnapshot:
    def test_empty_snapshot(self):
        snap = SystemSnapshot()
        assert snap.services == {}
        assert snap.timestamp > 0

    def test_add_service_metrics(self):
        snap = SystemSnapshot()
        snap.services["ray"] = ServiceMetrics(service="ray", healthy=True, data={"actors": []})
        assert snap.services["ray"].healthy is True


class TestActionResult:
    def test_success(self):
        r = ActionResult(success=True, detail="ok")
        assert r.success is True
        assert r.error is None

    def test_failure(self):
        r = ActionResult(success=False, error="boom")
        assert r.success is False
        assert r.error == "boom"
