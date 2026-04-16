from datetime import datetime

from etl.agents.hub import AgentHub, AgentInfo


def _make_hub() -> AgentHub:
    hub = AgentHub.__ray_actor_class__.__new__(AgentHub.__ray_actor_class__)
    hub._agents = {}
    hub._capability_index = {}
    hub._stats = {"total_registrations": 0}
    return hub


def test_is_alive_prunes_stale_registration(monkeypatch) -> None:
    hub = _make_hub()
    hub._agents["stale-agent"] = AgentInfo(
        name="stale-agent",
        capabilities=["demo.capability"],
        capability_specs=[{"id": "demo.capability", "aliases": []}],
        registered_at=datetime.utcnow(),
        metadata={},
    )
    hub._capability_index = {"demo.capability": ["stale-agent"]}

    import ray.serve as serve

    def missing(_name: str):
        raise ValueError("not found")

    monkeypatch.setattr(serve, "get_app_handle", missing)

    assert hub._is_alive("stale-agent") is False
    assert "stale-agent" not in hub._agents
    assert hub._capability_index["demo.capability"] == []


def test_is_alive_does_not_prune_on_temporary_handle_failure(monkeypatch) -> None:
    hub = _make_hub()
    hub._agents["temp-agent"] = AgentInfo(
        name="temp-agent",
        capabilities=["demo.capability"],
        capability_specs=[{"id": "demo.capability", "aliases": []}],
        registered_at=datetime.utcnow(),
        metadata={},
    )
    hub._capability_index = {"demo.capability": ["temp-agent"]}

    import ray.serve as serve

    def boom(_name: str):
        raise RuntimeError("temporary ray serve outage")

    monkeypatch.setattr(serve, "get_app_handle", boom)

    assert hub._is_alive("temp-agent") is False
    assert "temp-agent" in hub._agents
    assert hub._capability_index["demo.capability"] == ["temp-agent"]


def test_list_agents_survives_stale_entries(monkeypatch) -> None:
    hub = _make_hub()
    hub._agents = {
        "healthy-agent": AgentInfo(
            name="healthy-agent",
            capabilities=["healthy.capability"],
            capability_specs=[{"id": "healthy.capability", "aliases": []}],
            registered_at=datetime.utcnow(),
            metadata={"class": "HealthyAgent"},
        ),
        "stale-agent": AgentInfo(
            name="stale-agent",
            capabilities=["stale.capability"],
            capability_specs=[{"id": "stale.capability", "aliases": []}],
            registered_at=datetime.utcnow(),
            metadata={"class": "StaleAgent"},
        ),
    }
    hub._capability_index = {
        "healthy.capability": ["healthy-agent"],
        "stale.capability": ["stale-agent"],
    }

    import ray.serve as serve

    def fake_get_handle(name: str):
        if name == "healthy-agent":
            return object()
        raise ValueError("not found")

    monkeypatch.setattr(serve, "get_app_handle", fake_get_handle)

    result = hub.list_agents()

    assert [row["name"] for row in result] == ["healthy-agent"]
    assert result[0]["alive"] is True
    assert "stale-agent" not in hub._agents


def test_query_and_health_check_survive_stale_entries(monkeypatch) -> None:
    hub = _make_hub()
    hub._agents = {
        "healthy-agent": AgentInfo(
            name="healthy-agent",
            capabilities=["demo.capability"],
            capability_specs=[{"id": "demo.capability", "aliases": ["demo"]}],
            registered_at=datetime.utcnow(),
            metadata={},
        ),
        "stale-agent": AgentInfo(
            name="stale-agent",
            capabilities=["demo.capability"],
            capability_specs=[{"id": "demo.capability", "aliases": ["demo"]}],
            registered_at=datetime.utcnow(),
            metadata={},
        ),
    }
    hub._capability_index = {"demo.capability": ["healthy-agent", "stale-agent"], "demo": ["healthy-agent", "stale-agent"]}

    import ray.serve as serve

    def fake_get_handle(name: str):
        if name == "healthy-agent":
            return object()
        raise ValueError("not found")

    monkeypatch.setattr(serve, "get_app_handle", fake_get_handle)

    query_result = hub.query_agents(capability="demo.capability", alive_only=True)
    health = hub.health_check()

    assert [row["name"] for row in query_result] == ["healthy-agent"]
    assert health == {"healthy-agent": True}
    assert "stale-agent" not in hub._agents
