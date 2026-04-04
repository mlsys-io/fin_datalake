"""
RayCollector — Probes the Ray Dashboard REST API.

Uses the Ray State API (HTTP) to gather actor health and cluster resources
without needing to be inside the Ray cluster.
"""

from __future__ import annotations

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

            # Parse actors into a clean summary
            actors = []
            for actor_id, info in actors_raw.get("data", {}).get("actors", {}).items():
                actor_name = info.get("name", "unknown")
                actors.append({
                    "actor_id": actor_id,
                    "name": actor_name,
                    "class_name": info.get("className", "unknown"),
                    "state": info.get("state", "UNKNOWN"),
                    "pid": info.get("pid"),
                    "serve_app_name": _extract_serve_app_name(actor_name),
                })

            alive = sum(1 for a in actors if a["state"] == "ALIVE")
            dead = sum(1 for a in actors if a["state"] == "DEAD")

            return ServiceMetrics(
                service="ray",
                healthy=True,
                data={
                    "actors": actors,
                    "actors_alive": alive,
                    "actors_dead": dead,
                    "cluster": cluster,
                },
            )
        except Exception as e:
            return ServiceMetrics(service="ray", healthy=False, error=str(e))
