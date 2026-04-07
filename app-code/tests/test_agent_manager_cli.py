from typer.testing import CliRunner

from etl.agents import manager
from etl.agents.cli import app


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


def test_cli_profiles_reports_baseline_fleet() -> None:
    result = runner.invoke(app, ["profiles"])

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
