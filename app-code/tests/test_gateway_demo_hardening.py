from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest
from fastapi.testclient import TestClient

import gateway.api.main as main_module
from gateway.api.deps import get_current_user, get_db, get_registry
import gateway.adapters.data as data_adapter_module
import gateway.adapters.system as system_adapter_module
import gateway.api.routers.agents as agents_router
import gateway.api.routers.auth as auth_router
import gateway.api.routers.intent as intent_router
import gateway.api.routers.system as system_router
import gateway.adapters.broker as broker_adapter_module
import gateway.adapters.agent as agent_adapter_module
import gateway.core.dispatch as dispatch_module
import gateway.core.redis as redis_module
import gateway.core.ray_client as ray_client_module
import gateway.core.rbac as rbac_module
import gateway.mcp.server as mcp_server
import gateway.services.interfaces as interfaces_service_module
import gateway.services.system as system_service_module
from gateway.core.adapters import AdapterExecutionError
from gateway.core.dispatch import CircuitBreakerOpenError
from gateway.core.rbac import Permission
from gateway.models.intent import UserIntent
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


def test_system_router_proxies_snapshot_payload(client: TestClient, monkeypatch) -> None:
    async def fake_dispatch(**kwargs):
        return type("Result", (), {"data": {"snapshots": [{"timestamp": "2026-04-05T00:00:00+00:00"}]}})()

    monkeypatch.setattr(system_router, "dispatch", fake_dispatch)

    response = client.get("/api/v1/system/overseer/snapshots?n=1")

    assert response.status_code == 200
    assert response.json() == [{"timestamp": "2026-04-05T00:00:00+00:00"}]


def test_system_router_uses_standardized_error_envelope(client: TestClient, monkeypatch) -> None:
    async def fake_dispatch(**_kwargs):
        raise ValueError("Bad system request")

    monkeypatch.setattr(system_router, "dispatch", fake_dispatch)

    response = client.get("/api/v1/system/interfaces")

    assert response.status_code == 400
    assert response.json() == {
        "detail": "Bad system request",
        "code": "invalid_request",
        "context": None,
    }


def test_system_router_exposes_audit_log_query(client: TestClient, monkeypatch) -> None:
    async def fake_dispatch(**kwargs):
        return type(
            "Result",
            (),
            {
                "data": {
                    "audit_logs": [
                        {
                            "request_id": "req-1",
                            "domain": "data",
                            "action": "run_sql",
                            "status_code": 200,
                        }
                    ],
                    "count": 1,
                    "returned_count": 1,
                    "query": {
                        "request_id": "req-1",
                        "since": "1h",
                        "limit": 1,
                    },
                }
            },
        )()

    monkeypatch.setattr(system_router, "dispatch", fake_dispatch)

    response = client.get("/api/v1/system/audit-logs?limit=1&since=1h&request_id=req-1")

    assert response.status_code == 200
    assert response.json() == {
        "audit_logs": [
            {
                "request_id": "req-1",
                "domain": "data",
                "action": "run_sql",
                "status_code": 200,
            }
        ],
        "count": 1,
        "returned_count": 1,
        "query": {
            "request_id": "req-1",
            "since": "1h",
            "limit": 1,
        },
    }


def test_system_router_exposes_interface_inventory(client: TestClient, monkeypatch) -> None:
    async def fake_dispatch(**kwargs):
        return type(
            "Result",
            (),
            {
                "data": {
                    "generated_at": "2026-04-05T00:00:00+00:00",
                    "summary": {"domains": 1, "actions": 1, "mcp_tools": 1, "routes": 1, "proxies": 1},
                }
            },
        )()

    monkeypatch.setattr(system_router, "dispatch", fake_dispatch)

    response = client.get("/api/v1/system/interfaces")

    assert response.status_code == 200
    assert response.json() == {
        "generated_at": "2026-04-05T00:00:00+00:00",
        "summary": {"domains": 1, "actions": 1, "mcp_tools": 1, "routes": 1, "proxies": 1},
    }


@pytest.mark.asyncio
async def test_probe_infra_targets_treats_404_as_unavailable(monkeypatch) -> None:
    monkeypatch.setenv("PREFECT_UI_URL", "http://prefect.local")
    monkeypatch.setenv("RAY_DASHBOARD_URL", "http://ray.local")
    monkeypatch.setenv("MINIO_CONSOLE_URL", "http://minio.local")

    class FakeResponse:
        def __init__(self, status_code: int):
            self.status_code = status_code

    class FakeClient:
        def __init__(self, *args, **kwargs):
            self._responses = {
                "http://prefect.local": FakeResponse(404),
                "http://ray.local": FakeResponse(401),
                "http://minio.local": FakeResponse(503),
            }

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url: str):
            return self._responses[url]

    monkeypatch.setattr(system_service_module.httpx, "AsyncClient", FakeClient)

    result = await system_service_module.probe_infra_targets()

    assert result["targets"]["prefect"]["ok"] is False
    assert result["targets"]["ray"]["ok"] is True
    assert result["targets"]["minio"]["ok"] is False


@pytest.mark.asyncio
async def test_agent_adapter_list_does_not_write_catalog_on_read(monkeypatch) -> None:
    adapter = agent_adapter_module.AgentAdapter()

    class DummySession:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    async def fake_list_agent_definitions(db, *, enabled_only=True):
        return []

    def fail_upsert(*args, **kwargs):
        raise AssertionError("list path should not write to the durable catalog")

    monkeypatch.setattr(agent_adapter_module, "AsyncSessionLocal", lambda: DummySession())
    monkeypatch.setattr(agent_adapter_module.crud, "list_agent_definitions", fake_list_agent_definitions)
    monkeypatch.setattr(agent_adapter_module.crud, "upsert_agent_definition", fail_upsert)

    result = await adapter._get_catalog_agents(
        [
            {
                "name": "live-agent",
                "alive": True,
                "capabilities": ["demo.capability"],
                "capability_specs": [{"id": "demo.capability", "aliases": []}],
            }
        ]
    )

    assert [row["name"] for row in result] == ["live-agent"]
    assert result[0]["source"] == "runtime"


@pytest.mark.asyncio
async def test_dispatch_marks_adapter_execution_error_as_bad_gateway(monkeypatch) -> None:
    captured: list[int] = []

    async def fake_log(*args, **kwargs):
        if len(args) >= 3:
            captured.append(args[2])
        else:
            captured.append(kwargs.get("status_code"))

    class FakeRegistry:
        async def route(self, user, intent):
            raise AdapterExecutionError("RisingWave query failed", error_type="RuntimeError")

    monkeypatch.setattr(dispatch_module, "_log_audit_event", fake_log)

    with pytest.raises(AdapterExecutionError):
        await dispatch_module.dispatch(
            registry=FakeRegistry(),
            user=User(username="tester", hashed_password="x", role_names=["Admin"], email="tester@example.com"),
            domain="data",
            action="query_stream",
            parameters={"sql": "SELECT 1"},
            source_protocol="rest",
        )

    assert captured == [502]


@pytest.mark.asyncio
async def test_dispatch_marks_generic_exception_as_internal_server_error(monkeypatch) -> None:
    captured: list[int] = []

    async def fake_log(*args, **kwargs):
        if len(args) >= 3:
            captured.append(args[2])
        else:
            captured.append(kwargs.get("status_code"))

    class FakeRegistry:
        async def route(self, user, intent):
            raise RuntimeError("unexpected failure")

    monkeypatch.setattr(dispatch_module, "_log_audit_event", fake_log)

    with pytest.raises(RuntimeError):
        await dispatch_module.dispatch(
            registry=FakeRegistry(),
            user=User(username="tester", hashed_password="x", role_names=["Admin"], email="tester@example.com"),
            domain="data",
            action="query_stream",
            parameters={"sql": "SELECT 1"},
            source_protocol="rest",
        )

    assert captured == [500]


def test_sanitize_audit_parameters_redacts_sensitive_values() -> None:
    sanitized = dispatch_module._sanitize_audit_parameters(  # noqa: SLF001
        {
            "password": "super-secret",
            "nested": {"api_key": "abc123", "note": "visible"},
            "items": [{"token": "tok-1"}, ("plain", {"secret": "shh"})],
            "long_text": "x" * 2001,
        }
    )

    assert sanitized["password"] == "[REDACTED]"
    assert sanitized["nested"]["api_key"] == "[REDACTED]"
    assert sanitized["nested"]["note"] == "visible"
    assert sanitized["items"][0]["token"] == "[REDACTED]"
    assert sanitized["items"][1][1]["secret"] == "[REDACTED]"
    assert sanitized["long_text"].endswith("[truncated]")


def test_data_adapter_rejects_write_sql() -> None:
    adapter = data_adapter_module.DataAdapter()

    with pytest.raises(ValueError, match="read-oriented"):
        adapter._validate_read_only_sql("DROP TABLE users")

    with pytest.raises(ValueError, match="Multiple SQL statements are not allowed"):
        adapter._validate_read_only_sql("SELECT 1; SELECT 2")


def test_broker_adapter_builds_encoded_postgres_connection_string(monkeypatch) -> None:
    adapter = broker_adapter_module.BrokerAdapter()
    monkeypatch.setattr(adapter, "_require_permission", lambda *args, **kwargs: None)
    monkeypatch.setenv("TIMESCALE_HOST", "timescale.local")
    monkeypatch.setenv("TIMESCALE_PORT", "5432")
    monkeypatch.setenv("TIMESCALE_DB", "etl/prod")
    monkeypatch.setenv("TIMESCALE_USER", "svc@example.com")
    monkeypatch.setenv("TIMESCALE_PASSWORD", "pa:ss@word")

    result = adapter._get_psql_string(
        User(username="tester", hashed_password="x", role_names=["Admin"], email="tester@example.com"),
        UserIntent(
            domain="broker",
            action="get_psql_string",
            parameters={},
            user_id="tester",
            roles=["Admin"],
        ),
    )

    assert result["service"] == "timescaledb"
    assert result["connection_string"] == "postgresql://svc%40example.com:pa%3Ass%40word@timescale.local:5432/etl%2Fprod"
    assert result["jdbc_url"] == "jdbc:postgresql://timescale.local:5432/etl%2Fprod"


def test_broker_adapter_rejects_missing_timescale_config(monkeypatch) -> None:
    adapter = broker_adapter_module.BrokerAdapter()
    monkeypatch.setattr(adapter, "_require_permission", lambda *args, **kwargs: None)
    monkeypatch.delenv("TIMESCALE_HOST", raising=False)
    monkeypatch.delenv("TIMESCALE_USER", raising=False)
    monkeypatch.delenv("TIMESCALE_PASSWORD", raising=False)

    with pytest.raises(AdapterExecutionError) as exc_info:
        adapter._get_psql_string(
            User(username="tester", hashed_password="x", role_names=["Admin"], email="tester@example.com"),
            UserIntent(
                domain="broker",
                action="get_psql_string",
                parameters={},
                user_id="tester",
                roles=["Admin"],
            ),
        )

    assert "TimescaleDB connection details is not configured" in str(exc_info.value)


def test_broker_adapter_rejects_missing_s3_config(monkeypatch) -> None:
    adapter = broker_adapter_module.BrokerAdapter()
    monkeypatch.setattr(adapter, "_require_permission", lambda *args, **kwargs: None)
    monkeypatch.delenv("MINIO_ENDPOINT", raising=False)
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)

    with pytest.raises(AdapterExecutionError) as exc_info:
        adapter._get_s3_creds(
            User(username="tester", hashed_password="x", role_names=["Admin"], email="tester@example.com"),
            UserIntent(
                domain="broker",
                action="get_s3_creds",
                parameters={},
                user_id="tester",
                roles=["Admin"],
            ),
        )

    assert "MinIO/S3 credentials is not configured" in str(exc_info.value)


@pytest.mark.asyncio
async def test_system_adapter_health_uses_read_permission(monkeypatch) -> None:
    adapter = system_adapter_module.SystemAdapter()
    calls: list[Permission] = []

    def fake_require(user, permission):
        calls.append(permission)

    monkeypatch.setattr(adapter, "_require_permission", fake_require)
    monkeypatch.setattr(redis_module, "get_redis_client", lambda: None)

    result = await adapter._get_health_async(
        User(username="tester", hashed_password="x", role_names=["Reader"], email="tester@example.com"),
        UserIntent(
            domain="system",
            action="health",
            parameters={},
            user_id="tester",
            roles=["Reader"],
        ),
    )

    assert result == {"status": "unknown", "message": "Redis not configured"}
    assert calls == [Permission.SYSTEM_READ]


@pytest.mark.asyncio
async def test_system_adapter_audit_logs_uses_read_permission(monkeypatch) -> None:
    adapter = system_adapter_module.SystemAdapter()
    calls: list[Permission] = []

    def fake_require(user, permission):
        calls.append(permission)

    async def fake_audit_logs(**kwargs):
        return {"audit_logs": [{"request_id": "req-1"}], "count": 1, "returned_count": 1, "query": kwargs}

    monkeypatch.setattr(adapter, "_require_permission", fake_require)
    monkeypatch.setattr(system_service_module, "fetch_audit_logs", fake_audit_logs)

    result = await adapter._get_audit_logs(
        User(username="tester", hashed_password="x", role_names=["Reader"], email="tester@example.com"),
        UserIntent(
            domain="system",
            action="audit_logs",
            parameters={
                "request_id": "req-1",
                "since": "1h",
                "limit": 25,
                "source_protocol": "rest",
                "domain": "data",
                "action": "run_sql",
                "status_code": 200,
                "user_id": "tester",
            },
            user_id="tester",
            roles=["Reader"],
        ),
    )

    assert result["count"] == 1
    assert result["returned_count"] == 1
    assert result["query"]["request_id"] == "req-1"
    assert result["query"]["limit"] == 25
    assert calls == [Permission.SYSTEM_READ]


@pytest.mark.asyncio
async def test_system_adapter_interface_inventory_uses_read_permission(monkeypatch) -> None:
    adapter = system_adapter_module.SystemAdapter()
    calls: list[Permission] = []

    def fake_require(user, permission):
        calls.append(permission)

    async def fake_inventory():
        return {"generated_at": "2026-04-05T00:00:00+00:00", "summary": {"domains": 1}}

    monkeypatch.setattr(adapter, "_require_permission", fake_require)
    monkeypatch.setattr(interfaces_service_module, "fetch_interface_inventory", fake_inventory)

    result = await adapter._get_interface_inventory(
        User(username="tester", hashed_password="x", role_names=["Reader"], email="tester@example.com"),
        UserIntent(
            domain="system",
            action="interface_inventory",
            parameters={},
            user_id="tester",
            roles=["Reader"],
        ),
    )

    assert result["summary"]["domains"] == 1
    assert calls == [Permission.SYSTEM_READ]


@pytest.mark.asyncio
async def test_interface_inventory_marks_routes_and_proxies(monkeypatch) -> None:
    class FakeAdapter:
        def describe_actions(self):
            return [
                {
                    "name": "inspect",
                    "description": "Inspect a surface",
                    "permission": Permission.SYSTEM_READ.value,
                    "protocols": ["rest", "mcp"],
                }
            ]

    class FakeRegistry:
        def registered_domains(self):
            return ["system"]

        def get_adapter(self, domain):
            return FakeAdapter()

    async def fake_probe():
        return {
            "targets": {
                "prefect": {
                    "ok": True,
                    "status_code": 200,
                    "url": "http://localhost:4200",
                    "detail": None,
                }
            }
        }

    monkeypatch.setattr(interfaces_service_module, "build_default_registry", lambda: FakeRegistry())
    monkeypatch.setattr(interfaces_service_module, "probe_infra_targets", fake_probe)

    inventory = await interfaces_service_module.fetch_interface_inventory()

    assert inventory["domains"][0]["state"] == "ready"
    assert inventory["domains"][0]["source"] == "registry"
    assert inventory["routes"][0]["state"] == "declared"
    assert inventory["routes"][0]["source"] == "gateway"
    assert inventory["proxies"][0]["state"] == "reachable"
    assert inventory["proxies"][0]["source"] == "probe"
    assert inventory["summary"]["reachable_proxies"] == 1


@pytest.mark.asyncio
async def test_data_adapter_catalog_sources_groups_live_sources(monkeypatch) -> None:
    adapter = data_adapter_module.DataAdapter()

    async def fake_list_tables(intent):
        return {
            "source": "hive",
            "tables": [
                {"name": "market_data", "path": "s3://delta-lake/bronze/market_data"},
            ],
        }

    async def fake_query_stream(intent):
        return {
            "columns": ["table_schema", "table_name"],
            "rows": [["public", "market_pulse_signals"]],
        }

    monkeypatch.delenv("TIMESCALE_HOST", raising=False)
    monkeypatch.delenv("TIMESCALE_USER", raising=False)
    monkeypatch.delenv("TIMESCALE_PASSWORD", raising=False)
    monkeypatch.setattr(adapter, "_list_tables", fake_list_tables)
    monkeypatch.setattr(adapter, "_query_stream", fake_query_stream)

    result = await adapter._catalog_sources(
        UserIntent(
            domain="data",
            action="catalog_sources",
            parameters={},
            user_id="tester",
            roles=["Reader"],
        )
    )

    assert result["summary"]["total_sources"] == 3
    assert any(source["id"] == "lakehouse" for source in result["live_sources"])
    assert any(source["id"] == "risingwave" for source in result["live_sources"])
    assert result["live_sources"][0]["source_family"] in {"lakehouse", "streaming_sql", "postgres"}
    assert all(table.get("family") for source in result["live_sources"] for table in source["tables"])


def test_system_adapter_logs_query_echoes_all_filters(monkeypatch) -> None:
    adapter = system_adapter_module.SystemAdapter()
    calls: list[Permission] = []

    def fake_require(user, permission):
        calls.append(permission)

    def fake_execute_query(sql, params):
        return []

    monkeypatch.setattr(adapter, "_require_permission", fake_require)
    monkeypatch.setattr(adapter, "_execute_query", fake_execute_query)

    result = adapter._query_logs(
        User(username="tester", hashed_password="x", role_names=["Reader"], email="tester@example.com"),
        UserIntent(
            domain="system",
            action="logs",
            parameters={
                "component": "gateway",
                "level": "info",
                "since": "1h",
                "agent_name": "market-analyst",
                "trace_id": "trace-1",
                "limit": 10,
            },
            user_id="tester",
            roles=["Reader"],
        ),
    )

    assert result["query"] == {
        "component": "gateway",
        "level": "info",
        "since": "1h",
        "agent_name": "market-analyst",
        "trace_id": "trace-1",
        "limit": 10,
    }
    assert calls == [Permission.SYSTEM_READ]


def test_mcp_tool_registry_collects_all_tools() -> None:
    tool_names = mcp_server._collect_tool_specs().names()

    assert len(tool_names) == len(set(tool_names))
    assert {
        "query_data",
        "query_stream",
        "preview_table",
        "list_catalog_sources",
        "submit_job",
        "chat_agent",
        "invoke_agent",
        "broadcast_agents",
        "get_system_health",
        "get_overseer_snapshots",
        "get_overseer_alerts",
        "get_infra_status",
        "query_audit_logs",
        "get_interface_inventory",
        "list_connections",
    }.issubset(set(tool_names))
