"""
Gateway-owned Ray client lifecycle helpers.

The gateway is a long-running web service, so it should establish one Ray
connection at startup and reuse it for request handling instead of lazily
re-initializing Ray inside request paths.
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Callable, TypeVar

logger = logging.getLogger(__name__)

_init_lock = threading.RLock()
_ray_ready = False
T = TypeVar("T")


def _reset_gateway_ray() -> None:
    """
    Reset cached gateway Ray state so the next initialization can reconnect.
    """
    global _ray_ready

    try:
        import ray

        if ray.is_initialized():
            ray.shutdown()
    except Exception:
        pass

    _ray_ready = False


def init_gateway_ray() -> bool:
    """
    Initialize the shared Ray client for the gateway process once.

    Returns True when Ray is ready, False when startup should continue in a
    degraded catalog-only mode.
    """
    global _ray_ready

    if _ray_ready:
        return True

    with _init_lock:
        if _ray_ready:
            return True

        try:
            import ray
            from etl.runtime import ensure_ray, resolve_ray_namespace

            if not ray.is_initialized():
                ensure_ray(
                    address=os.getenv("RAY_ADDRESS", "auto"),
                    namespace=resolve_ray_namespace(os.getenv("RAY_NAMESPACE")),
                )

            _ray_ready = ray.is_initialized()
            return _ray_ready
        except Exception as exc:
            logger.warning("Gateway Ray initialization failed: %s", exc, exc_info=True)
            _ray_ready = False
            return False


def _retry_gateway_ray(operation_name: str, fn):
    """
    Run a Ray-backed operation once, then reset/reconnect and retry on failure.
    """
    try:
        return fn()
    except Exception as exc:
        logger.warning(
            "Gateway Ray operation '%s' failed, attempting reconnect: %s",
            operation_name,
            exc,
            exc_info=True,
        )
        with _init_lock:
            _reset_gateway_ray()
            if not init_gateway_ray():
                raise
        return fn()


def run_gateway_ray_operation(operation_name: str, fn: Callable[[], T]) -> T:
    """
    Run a Ray-backed gateway operation with reconnect-once behavior.
    """
    if not init_gateway_ray():
        raise RuntimeError("Gateway Ray client is not initialized")
    return _retry_gateway_ray(operation_name, fn)


def get_gateway_hub():
    """
    Return the shared AgentHub handle using the gateway-owned Ray client.
    """
    from etl.agents.hub import get_hub

    return run_gateway_ray_operation(
        "get_gateway_hub",
        lambda: get_hub(create_if_missing=False),
    )


def get_gateway_agent_handle(agent_name: str):
    """
    Return a Ray Serve app handle using the gateway-owned Ray client.
    """
    import ray.serve as serve

    return run_gateway_ray_operation(
        f"get_gateway_agent_handle:{agent_name}",
        lambda: serve.get_app_handle(agent_name),
    )
