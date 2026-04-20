from __future__ import annotations

import os
from typing import Any


DEFAULT_NAMESPACE = os.environ.get("RAY_NAMESPACE", "serve")


def _state_api_address(address: str) -> str | None:
    if address and address != "auto" and address.startswith(("http://", "https://")):
        return address

    from etl.config import config

    dashboard_url = str(config.RAY_DASHBOARD_URL or "").strip()
    if dashboard_url:
        return dashboard_url

    if address and address != "auto" and not address.startswith("ray://"):
        return address

    return None


def list_ray_actors(*, namespace: str = DEFAULT_NAMESPACE, address: str = "auto") -> dict[str, Any]:
    from ray.util.state import list_actors

    state_address = _state_api_address(address)
    actors = list_actors(filters=[("state", "=", "ALIVE")], address=state_address)
    namespace_value = str(namespace or "").strip()
    if namespace_value:
        actors = [
            actor for actor in actors
            if actor.get("ray_namespace") in {namespace_value, None, ""}
        ]
    return {
        "address": address,
        "state_api_address": state_address,
        "namespace": namespace,
        "actors": [
            {
                "name": actor.get("name", "Unknown"),
                "class_name": actor.get("class_name", "-"),
                "state": actor.get("state", "UNKNOWN"),
                "namespace": actor.get("ray_namespace", "-"),
                "actor_id": actor.get("actor_id", "-"),
                "node_ip": actor.get("address", {}).get("ip_address", "-"),
            }
            for actor in actors
        ],
    }
