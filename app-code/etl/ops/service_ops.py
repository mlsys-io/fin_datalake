from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from loguru import logger

from etl.context.store import delete_context_keys
from etl.runtime import ensure_ray


def delete_named_service(
    service_name: str,
    *,
    context_keys: Iterable[str] = (),
    address: str | None = None,
) -> dict[str, Any]:
    import ray

    ensure_ray(address=address)
    resolved_service_name = str(service_name).strip()
    if not resolved_service_name:
        raise ValueError("service_name must not be empty")

    requested_context_keys = [str(key).strip() for key in context_keys if str(key).strip()]
    result: dict[str, Any] = {
        "service_name": resolved_service_name,
        "service_found": False,
        "service_killed": False,
        "context_keys_requested": requested_context_keys,
        "context_keys_cleared": [],
        "context_keys_missing": [],
    }

    try:
        actor = ray.get_actor(resolved_service_name)
        result["service_found"] = True
        ray.kill(actor, no_restart=True)
        result["service_killed"] = True
        logger.info(f"[ServiceOps] Deleted service '{resolved_service_name}'.")
    except Exception as exc:
        logger.debug(f"[ServiceOps] No active service found for '{resolved_service_name}': {exc}")

    if requested_context_keys:
        cleared = delete_context_keys(requested_context_keys)
        result["context_keys_cleared"] = [key for key, deleted in cleared.items() if deleted]
        result["context_keys_missing"] = [key for key, deleted in cleared.items() if not deleted]

    return result
