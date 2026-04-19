from overseer.collectors.ray import map_serve_applications_by_name, parse_serve_applications
from overseer.config import load_endpoints
from overseer.actuators.ray_ops import RayActuator
from overseer.agent_registry import (
    UnrecoverableAgentResolutionError,
    resolve_agent_import_target,
)
from overseer.loop import Overseer
from overseer.models import (
    ActionResult,
    ActionType,
    OverseerAction,
    ServiceMetrics,
    SystemSnapshot,
)
from overseer.policies.healing import ActorHealthPolicy
import pytest


def _make_overseer() -> Overseer:
    return Overseer.__new__(Overseer)


def test_parse_serve_applications_normalizes_dashboard_payload() -> None:
    payload = {
        "applications": {
            "SupportAgent": {
                "status": "RUNNING",
                "route_prefix": "/SupportAgent",
                "deployments": {
                    "SupportAgent": {
                        "status": "HEALTHY",
                        "replica_states": {"RUNNING": 1},
                    }
                },
            },
            "ForecastModel-1": {
                "app_status": {"status": "DEPLOYING"},
                "route_prefix": "/ForecastModel-1",
                "deployments": {
                    "ForecastModel-1": {
                        "status": {"status": "UPDATING"},
                        "replicas": [{"state": "STARTING"}],
                    }
                },
            },
        }
    }

    applications = parse_serve_applications(payload)
    assert {app["name"] for app in applications} == {"SupportAgent", "ForecastModel-1"}

    support = next(app for app in applications if app["name"] == "SupportAgent")
    assert support["status"] == "RUNNING"
    assert support["route_prefix"] == "/SupportAgent"
    assert support["replica_counts"] == {"RUNNING": 1}
    assert support["observed_status"] == "ready"
    assert support["health_status"] == "healthy"
    assert support["recovery_state"] == "idle"

    forecast = next(app for app in applications if app["name"] == "ForecastModel-1")
    assert forecast["status"] == "DEPLOYING"
    assert forecast["replica_counts"] == {"STARTING": 1}
    assert forecast["observed_status"] == "recovering"
    assert forecast["health_status"] == "degraded"
    assert forecast["recovery_state"] == "recovering"

    by_name = map_serve_applications_by_name(applications)
    assert set(by_name) == {"SupportAgent", "ForecastModel-1"}
    assert by_name["SupportAgent"]["observed_status"] == "ready"
    assert by_name["ForecastModel-1"]["recovery_state"] == "recovering"


def test_normalize_catalog_deployment_uses_metadata_app_name_match() -> None:
    overseer = _make_overseer()
    deployment = overseer._normalize_catalog_deployment(
        entry={
            "name": "support-alias",
            "metadata": {"app_name": "SupportAgent"},
            "deployment_metadata": {},
            "desired_status": "running",
            "observed_status": "unknown",
            "health_status": "unknown",
            "recovery_state": "idle",
            "managed_by_overseer": True,
        },
        ray_available=True,
        app_state={
            "name": "SupportAgent",
            "route_prefix": "/SupportAgent",
            "running_replicas": 1,
            "unhealthy_replicas": 0,
            "observed_status": "ready",
            "health_status": "healthy",
            "recovery_state": "idle",
            "failure_reason": None,
            "notes": "Deployment is healthy in Ray Serve.",
        },
    )

    assert deployment["name"] == "SupportAgent"
    assert deployment["route_prefix"] == "/SupportAgent"
    assert deployment["observed_status"] == "ready"
    assert deployment["health_status"] == "healthy"
    assert deployment["recovery_state"] == "idle"
    assert deployment["last_failure_reason"] is None
    assert deployment["alive"] is True


def test_normalize_catalog_deployment_marks_missing_only_when_app_absent() -> None:
    overseer = _make_overseer()
    deployment = overseer._normalize_catalog_deployment(
        entry={
            "name": "SentimentModel-1",
            "metadata": {"app_name": "SentimentModel-1"},
            "deployment_metadata": {},
            "desired_status": "running",
            "observed_status": "ready",
            "health_status": "healthy",
            "recovery_state": "idle",
            "managed_by_overseer": True,
        },
        ray_available=True,
        app_state={},
    )

    assert deployment["observed_status"] == "missing"
    assert deployment["health_status"] == "offline"
    assert deployment["alive"] is False


def test_normalize_catalog_deployment_preserves_blocked_recovery() -> None:
    overseer = _make_overseer()
    deployment = overseer._normalize_catalog_deployment(
        entry={
            "name": "MarketAnalyst-1",
            "metadata": {
                "class": "MarketAnalystAgent",
                "class_path": "sample_agents.market_analyst:MarketAnalystAgent",
                "app_name": "MarketAnalyst-1",
            },
            "deployment_metadata": {},
            "desired_status": "running",
            "observed_status": "missing",
            "health_status": "offline",
            "recovery_state": "blocked",
            "last_failure_reason": "Catalog class_path could not be imported.",
            "reconcile_notes": "Recovery blocked by contract mismatch.",
            "managed_by_overseer": True,
        },
        ray_available=True,
        app_state={},
    )

    assert deployment["class_path"] == "sample_agents.market_analyst:MarketAnalystAgent"
    assert deployment["observed_status"] == "missing"
    assert deployment["health_status"] == "offline"
    assert deployment["recovery_state"] == "blocked"
    assert deployment["last_failure_reason"] == "Catalog class_path could not be imported."
    assert deployment["alive"] is False


def test_actor_health_policy_uses_class_path_and_skips_blocked() -> None:
    snapshot = SystemSnapshot(
        services={
            "agent_control": ServiceMetrics(
                service="agent_control",
                healthy=False,
                data={
                    "deployments": [
                        {
                            "name": "MarketAnalyst-1",
                            "metadata": {
                                "class": "MarketAnalystAgent",
                                "class_path": "sample_agents.market_analyst:MarketAnalystAgent",
                                "app_name": "MarketAnalyst-1",
                            },
                            "deployment_metadata": {"replication_mode": "serve"},
                            "desired_status": "running",
                            "observed_status": "missing",
                            "health_status": "offline",
                            "recovery_state": "idle",
                            "managed_by_overseer": True,
                        },
                        {
                            "name": "Strategy-1",
                            "metadata": {
                                "class": "StrategyAgent",
                                "class_path": "sample_agents.strategy_agent:StrategyAgent",
                                "app_name": "Strategy-1",
                            },
                            "deployment_metadata": {"replication_mode": "serve"},
                            "desired_status": "running",
                            "observed_status": "missing",
                            "health_status": "offline",
                            "recovery_state": "blocked",
                            "managed_by_overseer": True,
                        },
                    ]
                },
            )
        }
    )

    actions = ActorHealthPolicy().evaluate(snapshot)

    assert len(actions) == 1
    assert actions[0].type == ActionType.RESPAWN
    assert actions[0].deployment_name == "MarketAnalyst-1"
    assert actions[0].class_path == "sample_agents.market_analyst:MarketAnalystAgent"


def test_catalog_class_path_rejects_file_path_targets() -> None:
    with pytest.raises(UnrecoverableAgentResolutionError, match="module path"):
        resolve_agent_import_target("sample_agents/market_analyst.py:MarketAnalystAgent")


@pytest.mark.asyncio
async def test_ray_respawn_marks_missing_class_path_unrecoverable(monkeypatch) -> None:
    import etl.runtime

    monkeypatch.setattr("overseer.actuators.ray_ops.get_redis_client", lambda: None)
    monkeypatch.setattr(etl.runtime, "ensure_ray", lambda *args, **kwargs: None)

    actuator = RayActuator()
    result = await actuator.execute(
        OverseerAction(
            type=ActionType.RESPAWN,
            target="ray",
            agent="MarketAnalystAgent",
            agent_class="MarketAnalystAgent",
            deployment_name="MarketAnalyst-1",
        )
    )

    assert result.success is False
    assert result.retryable is False
    assert "Missing catalog class_path" in str(result.error)


@pytest.mark.asyncio
async def test_apply_action_result_blocks_unrecoverable_failures(monkeypatch) -> None:
    calls = []

    def fake_update_agent_catalog_status(**kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(
        "overseer.loop.update_agent_catalog_status",
        fake_update_agent_catalog_status,
    )

    overseer = _make_overseer()
    await overseer._apply_action_result(
        OverseerAction(
            type=ActionType.RESPAWN,
            target="ray",
            agent="MarketAnalystAgent",
            agent_class="MarketAnalystAgent",
            deployment_name="MarketAnalyst-1",
        ),
        ActionResult(
            success=False,
            error="Catalog class_path could not be imported.",
            retryable=False,
        ),
    )

    assert calls
    assert calls[0]["recovery_state"] == "blocked"
    assert calls[0]["health_status"] == "offline"
    assert calls[0]["last_failure_reason"] == "Catalog class_path could not be imported."


def test_load_endpoints_rejects_invalid_port_override(monkeypatch, tmp_path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
services:
  ray:
    host: localhost
    port: 8080
    protocol: http
""".strip()
    )
    monkeypatch.setenv("OVERSEER_CONFIG_PATH", str(config_path))
    monkeypatch.setenv("OVERSEER_RAY_PORT", "not-a-port")

    with pytest.raises(ValueError, match="OVERSEER_RAY_PORT must be a valid integer port"):
        load_endpoints()
