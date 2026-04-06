"""Deployment-aware utilities for managing Ray Serve agent fleets."""

from __future__ import annotations

from dataclasses import dataclass, field
import importlib
from typing import Any

from etl.agents.catalog import delete_agent_catalog_entry, list_agent_catalog_entries
from etl.runtime import ensure_ray


@dataclass(frozen=True)
class DeploymentSpec:
    class_name: str
    name: str
    num_replicas: int = 1
    num_cpus: float = 0.5
    config: dict[str, Any] = field(default_factory=dict)
    serve_options: dict[str, Any] = field(default_factory=dict)


BASELINE_FLEET: tuple[DeploymentSpec, ...] = (
    DeploymentSpec(class_name="SupportAgent", name="SupportAgent"),
    DeploymentSpec(class_name="SentimentModelAgent", name="SentimentModel-1"),
    DeploymentSpec(class_name="ForecastAgent", name="ForecastModel-1"),
    DeploymentSpec(class_name="RouterAgent", name="RouterAgent"),
)


def resolve_agent_class(class_name: str):
    from overseer.agent_registry import resolve_agent_class as resolve_from_registry

    resolved = resolve_from_registry(class_name)
    if resolved is not None:
        return resolved

    for module_name in ("agents", "etl.agents", "sample_agents"):
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            continue
        resolved = getattr(module, class_name, None)
        if resolved is not None:
            return resolved
    return None


def baseline_fleet_specs() -> list[DeploymentSpec]:
    return list(BASELINE_FLEET)


def deploy_agent(
    class_name: str,
    *,
    name: str,
    num_replicas: int = 1,
    num_cpus: float = 0.5,
    config: dict[str, Any] | None = None,
    serve_options: dict[str, Any] | None = None,
):
    ensure_ray()
    agent_cls = resolve_agent_class(class_name)
    if agent_cls is None:
        raise ValueError(f"Unknown agent class: {class_name}")

    return agent_cls.deploy(
        name=name,
        num_replicas=num_replicas,
        num_cpus=num_cpus,
        config=config or {},
        **(serve_options or {}),
    )


def deploy_fleet(specs: list[DeploymentSpec] | tuple[DeploymentSpec, ...]) -> dict[str, Any]:
    handles: dict[str, Any] = {}
    for spec in specs:
        handles[spec.name] = deploy_agent(
            spec.class_name,
            name=spec.name,
            num_replicas=spec.num_replicas,
            num_cpus=spec.num_cpus,
            config=dict(spec.config),
            serve_options=dict(spec.serve_options),
        )
    return handles


def deploy_baseline_fleet() -> dict[str, Any]:
    return deploy_fleet(BASELINE_FLEET)


def delete_agent(name: str, *, clean_catalog: bool = True) -> dict[str, bool]:
    ensure_ray()
    import ray.serve as serve

    serve_deleted = False
    catalog_deleted = False

    try:
        serve.delete(name, _blocking=True)
        serve_deleted = True
    except Exception:
        serve_deleted = False

    if clean_catalog:
        try:
            catalog_deleted = delete_agent_catalog_entry(name)
        except Exception:
            catalog_deleted = False

    return {
        "serve_deleted": serve_deleted,
        "catalog_deleted": catalog_deleted,
    }


def delete_agent_deployment(name: str, *, clean_catalog: bool = True) -> dict[str, bool]:
    """Backward-compatible alias for callers using the older helper name."""
    return delete_agent(name, clean_catalog=clean_catalog)


def delete_fleet(
    specs: list[DeploymentSpec] | tuple[DeploymentSpec, ...],
    *,
    clean_catalog: bool = True,
) -> dict[str, dict[str, bool]]:
    return {
        spec.name: delete_agent(spec.name, clean_catalog=clean_catalog)
        for spec in specs
    }


def delete_baseline_fleet(*, clean_catalog: bool = True) -> dict[str, dict[str, bool]]:
    return delete_fleet(BASELINE_FLEET, clean_catalog=clean_catalog)


def list_fleet_state() -> dict[str, Any]:
    runtime_agents: list[dict[str, Any]] = []
    runtime_available = False
    runtime_error = None

    try:
        import ray
        from etl.agents.hub import get_hub

        ensure_ray()
        hub = get_hub(create_if_missing=False)
        runtime_agents = ray.get(hub.list_agents.remote())
        runtime_available = True
    except Exception as exc:
        runtime_error = str(exc)

    catalog_agents = list_agent_catalog_entries(runtime_source="ray-serve", enabled_only=True)
    runtime_by_name = {str(agent.get("name") or "").strip(): dict(agent) for agent in runtime_agents}
    catalog_by_name = {str(agent.get("name") or "").strip(): dict(agent) for agent in catalog_agents}

    merged = []
    for name in sorted({*catalog_by_name.keys(), *runtime_by_name.keys()}):
        if not name:
            continue
        catalog_agent = catalog_by_name.get(name, {})
        runtime_agent = runtime_by_name.get(name, {})
        merged.append(
            {
                "name": name,
                "class_name": str(
                    runtime_agent.get("metadata", {}).get("class")
                    or catalog_agent.get("metadata", {}).get("class")
                    or ""
                ),
                "capabilities": runtime_agent.get("capabilities") or catalog_agent.get("capabilities", []),
                "desired_status": catalog_agent.get("desired_status"),
                "observed_status": catalog_agent.get("observed_status"),
                "health_status": catalog_agent.get("health_status"),
                "alive": bool(runtime_agent.get("alive", catalog_agent.get("alive", False))),
                "source": (
                    "catalog+runtime"
                    if catalog_agent and runtime_agent
                    else "runtime"
                    if runtime_agent
                    else "catalog"
                ),
            }
        )

    return {
        "baseline_fleet": [spec.__dict__.copy() for spec in BASELINE_FLEET],
        "runtime_available": runtime_available,
        "runtime_error": runtime_error,
        "catalog": catalog_agents,
        "runtime": runtime_agents,
        "merged": merged,
    }
