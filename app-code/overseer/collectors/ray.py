"""
RayCollector — Probes the Ray Dashboard REST API.

Uses the Ray State API (HTTP) to gather actor health and cluster resources
without needing to be inside the Ray cluster.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
import httpx

from overseer.collectors.base import BaseCollector
from overseer.models import ServiceMetrics


def _extract_serve_app_name(actor_name: str) -> str | None:
    if not actor_name.startswith("ServeReplica:"):
        return None
    parts = actor_name.split(":")
    if len(parts) >= 3 and parts[1].strip():
        return parts[1].strip()
    return None


def _extract_status_token(value) -> str:
    if isinstance(value, Mapping):
        for key in ("status", "state"):
            nested = value.get(key)
            if nested:
                return str(nested).strip().upper()
        return ""
    if value in (None, ""):
        return ""
    return str(value).strip().upper()


def _extract_deployment_status(info: Mapping[str, object]) -> str:
    for key in ("status", "deployment_status", "message", "state"):
        token = _extract_status_token(info.get(key))
        if token:
            return token
    status_payload = info.get("status")
    return _extract_status_token(status_payload)


def _extract_replica_counts(info: Mapping[str, object]) -> dict[str, int]:
    replica_counts: dict[str, int] = {}

    replica_states = info.get("replica_states")
    if isinstance(replica_states, Mapping):
        for state, count in replica_states.items():
            token = _extract_status_token(state)
            if not token:
                continue
            try:
                replica_counts[token] = replica_counts.get(token, 0) + int(count)
            except (TypeError, ValueError):
                continue

    replicas = info.get("replicas")
    if isinstance(replicas, list):
        for replica in replicas:
            if not isinstance(replica, Mapping):
                continue
            token = _extract_status_token(replica.get("state") or replica.get("status"))
            if not token:
                continue
            replica_counts[token] = replica_counts.get(token, 0) + 1

    return replica_counts


def _summarize_serve_application(
    *,
    name: str,
    app_status: str,
    route_prefix,
    deployments: list[dict[str, object]],
    replica_counts: dict[str, int],
    alive_actor_replicas: int = 0,
) -> dict[str, object]:
    running_replicas = 0
    unhealthy_replicas = 0
    for state, count in replica_counts.items():
        if state in {"RUNNING", "HEALTHY"}:
            running_replicas += count
        else:
            unhealthy_replicas += count

    deployment_states = [str(deployment.get("status") or "").upper() for deployment in deployments]
    healthy_statuses = {"RUNNING", "HEALTHY"}
    recovering_statuses = {"DEPLOYING", "UPDATING", "STARTING", "RESTARTING"}
    deleting_statuses = {"DELETING", "STOPPING", "STOPPED", "DELETING_APP"}
    degraded_statuses = {"UNHEALTHY", "DEPLOY_FAILED", "FAILED"}

    any_recovering = app_status in recovering_statuses or any(
        state in recovering_statuses for state in deployment_states
    )
    any_deleting = app_status in deleting_statuses or any(
        state in deleting_statuses for state in deployment_states
    )
    any_degraded = app_status in degraded_statuses or any(
        state in degraded_statuses for state in deployment_states
    )
    serve_explicitly_healthy = (
        running_replicas > 0
        and app_status in healthy_statuses.union(recovering_statuses, {""})
    )

    # Ray's actor listing can lag or transiently miss freshly started Serve
    # replicas even when the Serve applications endpoint already reports a
    # healthy running replica set. Treat explicit healthy Serve replica state
    # as authoritative enough to avoid repeated false respawns, while still
    # using actor cross-checks to catch stale deleting application summaries.
    if serve_explicitly_healthy:
        effective_running_replicas = running_replicas
    elif running_replicas > 0:
        effective_running_replicas = min(running_replicas, alive_actor_replicas)
    else:
        effective_running_replicas = alive_actor_replicas

    if any_deleting:
        observed_status = "missing"
        health_status = "offline"
        recovery_state = "idle"
        failure_reason = f"Deployment '{name}' is being deleted or stopped by Ray Serve."
        notes = "Ray Serve reports the application as deleting or stopped."
    elif any_recovering and effective_running_replicas <= 0:
        observed_status = "recovering"
        health_status = "degraded"
        recovery_state = "recovering"
        failure_reason = None
        notes = "Deployment is actively being reconciled by Ray Serve."
    elif effective_running_replicas > 0 and (unhealthy_replicas > 0 or any_recovering or any_degraded):
        observed_status = "degraded"
        health_status = "degraded"
        recovery_state = "idle"
        failure_reason = None
        notes = "Deployment is partially available in Ray Serve."
    elif effective_running_replicas > 0 and app_status in healthy_statuses.union(recovering_statuses, {""}):
        observed_status = "ready"
        health_status = "healthy"
        recovery_state = "idle"
        failure_reason = None
        notes = "Deployment is healthy in Ray Serve."
    elif running_replicas > 0 and alive_actor_replicas <= 0 and not any_recovering:
        observed_status = "missing"
        health_status = "offline"
        recovery_state = "idle"
        failure_reason = (
            f"Serve reported application '{name}', but no live replica actors were found."
        )
        notes = "Serve application summary appears stale relative to the live actor list."
    elif any_degraded:
        observed_status = "degraded"
        health_status = "degraded"
        recovery_state = "failed"
        failure_reason = f"Ray Serve reports deployment '{name}' in a degraded state."
        notes = "Ray Serve reports application or deployment health issues."
    else:
        observed_status = "unknown"
        health_status = "unknown"
        recovery_state = "idle"
        failure_reason = None
        notes = "Ray Serve reported the application, but its state could not be classified."

    return {
        "name": name,
        "status": app_status,
        "route_prefix": route_prefix,
        "deployments": deployments,
        "replica_counts": replica_counts,
        "running_replicas": effective_running_replicas,
        "reported_running_replicas": running_replicas,
        "alive_actor_replicas": alive_actor_replicas,
        "unhealthy_replicas": unhealthy_replicas,
        "observed_status": observed_status,
        "health_status": health_status,
        "recovery_state": recovery_state,
        "failure_reason": failure_reason,
        "notes": notes,
    }


def parse_serve_applications(payload: Mapping[str, object] | None) -> list[dict[str, object]]:
    raw = payload or {}
    applications = raw.get("applications")
    if applications is None and isinstance(raw.get("data"), Mapping):
        applications = raw["data"].get("applications")
    if applications is None and "name" in raw:
        applications = [raw]

    normalized: list[dict[str, object]] = []
    if isinstance(applications, Mapping):
        iterable = applications.items()
    elif isinstance(applications, list):
        iterable = ((str(item.get("name") or ""), item) for item in applications if isinstance(item, Mapping))
    else:
        iterable = []

    for app_name, raw_app in iterable:
        if not isinstance(raw_app, Mapping):
            continue
        name = str(raw_app.get("name") or app_name or "").strip()
        if not name:
            continue

        app_status = _extract_status_token(raw_app.get("status"))
        if not app_status:
            app_status = _extract_status_token(raw_app.get("app_status"))

        route_prefix = raw_app.get("route_prefix")
        deployments_raw = raw_app.get("deployments") or {}
        deployments: list[dict[str, object]] = []
        total_replica_counts: dict[str, int] = {}

        if isinstance(deployments_raw, Mapping):
            deployment_items = deployments_raw.items()
        elif isinstance(deployments_raw, list):
            deployment_items = (
                (str(item.get("name") or item.get("deployment_name") or ""), item)
                for item in deployments_raw
                if isinstance(item, Mapping)
            )
        else:
            deployment_items = []

        for deployment_name, raw_deployment in deployment_items:
            if not isinstance(raw_deployment, Mapping):
                continue
            normalized_name = str(
                raw_deployment.get("name")
                or raw_deployment.get("deployment_name")
                or deployment_name
                or ""
            ).strip()
            if not normalized_name:
                continue

            deployment_status = _extract_deployment_status(raw_deployment)
            replica_counts = _extract_replica_counts(raw_deployment)
            for state, count in replica_counts.items():
                total_replica_counts[state] = total_replica_counts.get(state, 0) + count

            deployments.append(
                {
                    "name": normalized_name,
                    "status": deployment_status,
                    "replica_counts": replica_counts,
                }
            )

        normalized.append(
            _summarize_serve_application(
                name=name,
                app_status=app_status,
                route_prefix=route_prefix,
                deployments=deployments,
                replica_counts=total_replica_counts,
            )
        )

    return normalized


def count_alive_serve_replica_actors(
    actors: list[dict[str, object]],
) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for actor in actors:
        if str(actor.get("state") or "").upper() != "ALIVE":
            continue
        serve_app_name = str(actor.get("serve_app_name") or "").strip()
        if not serve_app_name:
            continue
        counts[serve_app_name] += 1
    return dict(counts)


def map_serve_applications_by_name(
    applications: list[dict[str, object]],
) -> dict[str, dict[str, object]]:
    return {
        str(app.get("name") or "").strip(): dict(app)
        for app in applications
        if str(app.get("name") or "").strip()
    }


def extract_actor_rows(payload: Mapping[str, object] | None) -> list[dict[str, object]]:
    raw = payload or {}
    data = raw.get("data")

    # Older dashboard shape: {"data": {"actors": {actor_id: {...}}}}
    if isinstance(data, Mapping):
        actors_map = data.get("actors")
        if isinstance(actors_map, Mapping):
            rows: list[dict[str, object]] = []
            for actor_id, info in actors_map.items():
                if not isinstance(info, Mapping):
                    continue
                rows.append(
                    {
                        "actor_id": str(actor_id),
                        "name": info.get("name", "unknown"),
                        "class_name": info.get("className") or info.get("class_name") or "unknown",
                        "state": info.get("state", "UNKNOWN"),
                        "pid": info.get("pid"),
                        "serve_app_name": _extract_serve_app_name(str(info.get("name", ""))),
                    }
                )
            return rows

        # Newer Ray shape: {"data": {"result": {"result": [{...}, ...]}}}
        result = data.get("result")
        if isinstance(result, Mapping):
            actor_list = result.get("result")
            if isinstance(actor_list, list):
                rows = []
                for info in actor_list:
                    if not isinstance(info, Mapping):
                        continue
                    actor_name = str(info.get("name") or "unknown")
                    class_name = str(
                        info.get("class_name")
                        or info.get("className")
                        or "unknown"
                    )
                    rows.append(
                        {
                            "actor_id": str(info.get("actor_id") or ""),
                            "name": actor_name,
                            "class_name": class_name,
                            "state": str(info.get("state") or "UNKNOWN"),
                            "pid": info.get("pid"),
                            "serve_app_name": _extract_serve_app_name(actor_name),
                        }
                    )
                return rows

    return []


class RayCollector(BaseCollector):

    async def collect(self) -> ServiceMetrics:
        base = f"http://{self.endpoint.host}:{self.endpoint.port}"
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                # Cluster resources (CPU, GPU, memory)
                res_resp = await client.get(f"{base}/api/v0/cluster_status")
                cluster = res_resp.json() if res_resp.status_code == 200 else {}

                # Actor listing
                actor_resp = await client.get(f"{base}/api/v0/actors")
                actors_raw = actor_resp.json() if actor_resp.status_code == 200 else {}

                # Serve applications
                serve_resp = await client.get(f"{base}/api/serve/applications/")
                serve_raw = serve_resp.json() if serve_resp.status_code == 200 else {}

            # Parse actors into a clean summary across Ray dashboard API shapes.
            actors = extract_actor_rows(actors_raw)

            alive = sum(1 for a in actors if a["state"] == "ALIVE")
            dead = sum(1 for a in actors if a["state"] == "DEAD")
            alive_serve_replica_counts = count_alive_serve_replica_actors(actors)
            serve_applications = [
                _summarize_serve_application(
                    name=str(app.get("name") or "").strip(),
                    app_status=str(app.get("status") or "").strip().upper(),
                    route_prefix=app.get("route_prefix"),
                    deployments=list(app.get("deployments") or []),
                    replica_counts=dict(app.get("replica_counts") or {}),
                    alive_actor_replicas=alive_serve_replica_counts.get(
                        str(app.get("name") or "").strip(),
                        0,
                    ),
                )
                for app in parse_serve_applications(serve_raw)
                if str(app.get("name") or "").strip()
            ]

            return ServiceMetrics(
                service="ray",
                healthy=True,
                data={
                    "actors": actors,
                    "actors_alive": alive,
                    "actors_dead": dead,
                    "serve_applications": serve_applications,
                    "serve_actor_replica_counts": alive_serve_replica_counts,
                    "serve_applications_by_name": map_serve_applications_by_name(
                        serve_applications
                    ),
                    "cluster": cluster,
                },
            )
        except Exception as e:
            return ServiceMetrics(service="ray", healthy=False, error=str(e))
