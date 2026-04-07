from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest
from fastapi.testclient import TestClient

import gateway.api.main as main_module
from gateway.api.deps import get_current_user, get_db, get_registry
import gateway.api.routers.agents as agents_router
import gateway.api.routers.auth as auth_router
import gateway.api.routers.intent as intent_router
import gateway.api.routers.system as system_router
import gateway.core.ray_client as ray_client_module
import gateway.core.rbac as rbac_module
from gateway.core.dispatch import CircuitBreakerOpenError
from gateway.models.user import User


@pytest.fixture
def app(monkeypatch):
    async def fake_load_roles():
        return {}

    async def fake_init_db():
        return None

    monkeypatch.setattr(rbac_module, "load_roles", fake_load_roles)
    monkeypatch.setattr(main_module, "init_db", fake_init_db)
    monkeypatch.setattr(ray_client_module, "init_gateway_ray", lambda: True)
    monkeypatch.setattr(main_module, "build_default_registry", lambda: object())

    app = main_module.create_app()

    async def fake_db() -> AsyncGenerator[None, None]:
        yield None

    app.dependency_overrides[get_db] = fake_db
    app.dependency_overrides[get_current_user] = lambda: User(
        username="tester",
        hashed_password="x",
        role_names=["Admin"],
        email="tester@example.com",
    )
    app.dependency_overrides[get_registry] = lambda: object()
    return app


@pytest.fixture
def client(app):
    with TestClient(app) as test_client:
        yield test_client


def test_healthz_returns_simple_liveness(client: TestClient) -> None:
    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_readyz_returns_structured_readiness_payload(client: TestClient, monkeypatch) -> None:
    async def fake_report(_app):
        return {
            "ready": False,
            "timestamp": "2026-04-05T00:00:00+00:00",
            "checks": {
                "database": {"ready": True, "detail": None},
                "registry": {"ready": True, "detail": None},
                "ray": {"ready": False, "detail": "Gateway Ray client is not initialized."},
                "redis": {"configured": False, "ready": False, "detail": "Redis URL is not configured."},
            },
        }

    monkeypatch.setattr(main_module, "build_readiness_report", fake_report)

    response = client.get("/readyz")

    assert response.status_code == 503
    body = response.json()
    assert body["ready"] is False
    assert body["checks"]["ray"]["ready"] is False


def test_auth_router_uses_standardized_error_envelope(client: TestClient, monkeypatch) -> None:
    async def fake_authenticate_user(_db, _username, _password):
        return None

    monkeypatch.setattr(auth_router.crud, "authenticate_user", fake_authenticate_user)

    response = client.post(
        "/api/v1/auth/login",
        json={"username": "wrong", "password": "wrong"},
    )

    assert response.status_code == 401
    assert response.json() == {
        "detail": "Incorrect username or password.",
        "code": "invalid_credentials",
        "context": None,
    }


def test_intent_router_uses_standardized_error_envelope(client: TestClient, monkeypatch) -> None:
    async def fake_dispatch(**_kwargs):
        raise ValueError("Bad intent payload")

    monkeypatch.setattr(intent_router, "dispatch", fake_dispatch)

    response = client.post(
        "/api/v1/intent",
        json={"domain": "data", "action": "query", "parameters": {}},
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "Bad intent payload",
        "code": "invalid_request",
        "context": None,
    }


def test_agents_router_uses_standardized_error_envelope(client: TestClient, monkeypatch) -> None:
    async def fake_dispatch(**_kwargs):
        raise CircuitBreakerOpenError("Agent plane paused")

    monkeypatch.setattr(agents_router, "dispatch", fake_dispatch)

    response = client.get("/api/v1/agents")

    assert response.status_code == 503
    assert response.json() == {
        "detail": "Agent plane paused",
        "code": "circuit_breaker_open",
        "context": None,
    }


def test_system_router_uses_standardized_error_envelope(client: TestClient, monkeypatch) -> None:
    async def fake_snapshots(_n: int):
        raise RuntimeError("Redis unavailable")

    monkeypatch.setattr(system_router, "fetch_overseer_snapshots", fake_snapshots)

    response = client.get("/api/v1/system/overseer/snapshots?n=1")

    assert response.status_code == 500
    assert response.json() == {
        "detail": "Redis unavailable",
        "code": "overseer_snapshots_unavailable",
        "context": None,
    }
