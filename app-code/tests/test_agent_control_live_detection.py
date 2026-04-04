from overseer.collectors.ray import parse_serve_applications
from overseer.loop import Overseer


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

    forecast = next(app for app in applications if app["name"] == "ForecastModel-1")
    assert forecast["status"] == "DEPLOYING"
    assert forecast["replica_counts"] == {"STARTING": 1}


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
            "status": "RUNNING",
            "route_prefix": "/SupportAgent",
            "deployments": [{"name": "SupportAgent", "status": "HEALTHY"}],
            "replica_counts": {"RUNNING": 1},
            "running_replicas": 1,
            "unhealthy_replicas": 0,
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
