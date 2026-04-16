from typer.testing import CliRunner

import etl.context.store as context_module
from etl.agents import manager
from etl.cli.main import app
import etl.ops.service_ops as service_ops
import etl.ops.status as status_ops


runner = CliRunner()


def test_baseline_fleet_specs_are_the_shared_source_of_truth() -> None:
    specs = manager.baseline_fleet_specs()

    assert [spec.name for spec in specs] == [
        "SupportAgent",
        "SentimentModel-1",
        "ForecastModel-1",
        "RouterAgent",
    ]
    assert [spec.class_name for spec in specs] == [
        "SupportAgent",
        "SentimentModelAgent",
        "ForecastAgent",
        "RouterAgent",
    ]


def test_cli_grouped_agent_profiles_reports_baseline_fleet() -> None:
    result = runner.invoke(app, ["agents", "profiles"])

    assert result.exit_code == 0
    assert "SupportAgent" in result.stdout
    assert "ForecastModel-1" in result.stdout


def test_delete_fleet_delegates_to_delete_agent(monkeypatch) -> None:
    calls: list[tuple[str, bool]] = []

    def fake_delete_agent(name: str, *, clean_catalog: bool = True):
        calls.append((name, clean_catalog))
        return {"serve_deleted": True, "catalog_deleted": clean_catalog}

    monkeypatch.setattr(manager, "delete_agent", fake_delete_agent)

    result = manager.delete_baseline_fleet(clean_catalog=False)

    assert sorted(result) == [
        "ForecastModel-1",
        "RouterAgent",
        "SentimentModel-1",
        "SupportAgent",
    ]
    assert calls == [
        ("SupportAgent", False),
        ("SentimentModel-1", False),
        ("ForecastModel-1", False),
        ("RouterAgent", False),
    ]


def test_grouped_delete_service_cli_uses_helper(monkeypatch) -> None:
    calls: list[tuple[str, tuple[str, ...], str | None]] = []

    def fake_delete_named_service(name: str, *, context_keys=(), address: str | None = None):
        calls.append((name, tuple(context_keys), address))
        return {
            "service_name": name,
            "service_found": True,
            "service_killed": True,
            "context_keys_requested": list(context_keys),
            "context_keys_cleared": list(context_keys),
            "context_keys_missing": [],
        }

    monkeypatch.setattr(service_ops, "delete_named_service", fake_delete_named_service)

    result = runner.invoke(app, ["services", "delete", "PriceSvc", "--context-key", "ctx:one"])

    assert result.exit_code == 0
    assert "PriceSvc" in result.stdout
    assert calls == [("PriceSvc", ("ctx:one",), None)]


def test_grouped_delete_context_cli_clears_context_key(monkeypatch) -> None:
    monkeypatch.setattr(context_module, "delete_context_keys", lambda keys: {key: key == "ctx:demo" for key in keys})

    result = runner.invoke(app, ["context", "delete", "ctx:demo"])

    assert result.exit_code == 0
    assert '"deleted": true' in result.stdout.lower()


def test_status_cli_reports_actor_inventory(monkeypatch) -> None:
    monkeypatch.setattr(
        status_ops,
        "list_ray_actors",
        lambda *, namespace, address: {
            "namespace": namespace,
            "address": address,
            "actors": [{"name": "AgentHub", "state": "ALIVE"}],
        },
    )

    result = runner.invoke(app, ["status", "actors", "--namespace", "demo", "--address", "local"])

    assert result.exit_code == 0
    assert "AgentHub" in result.stdout
    assert "demo" in result.stdout
