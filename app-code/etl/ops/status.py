from __future__ import annotations

from typing import Any


DEFAULT_NAMESPACE = "zdb_etl_service"


def list_ray_actors(*, namespace: str = DEFAULT_NAMESPACE, address: str = "auto") -> dict[str, Any]:
    import ray
    from ray.util.state import list_actors

    ray.init(address=address, namespace=namespace, ignore_reinit_error=True)
    actors = list_actors(filters=[("state", "=", "ALIVE")])
    return {
        "address": address,
        "namespace": namespace,
        "actors": [
            {
                "name": actor.get("name", "Unknown"),
                "state": actor.get("state", "UNKNOWN"),
                "actor_id": actor.get("actor_id", "-"),
                "node_ip": actor.get("address", {}).get("ip_address", "-"),
            }
            for actor in actors
        ],
    }

