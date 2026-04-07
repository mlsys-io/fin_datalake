"""
Gateway-owned Ray client lifecycle helpers.

The gateway is a long-running web service, so it should establish one Ray
connection on a dedicated worker thread and route all Ray-backed operations
through that same thread. This avoids Ray Client thread-local context issues
inside async request handlers.
"""

from __future__ import annotations

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

_init_lock = threading.RLock()
_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="gateway-ray")
_ray_ready = False


def _run_on_ray_thread(fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    return _executor.submit(fn, *args, **kwargs).result()


def _init_gateway_ray_sync() -> bool:
    import ray
    from etl.runtime import ensure_ray, resolve_ray_namespace

    if not ray.is_initialized():
        ensure_ray(
            address=os.getenv("RAY_ADDRESS", "auto"),
            namespace=resolve_ray_namespace(os.getenv("RAY_NAMESPACE")),
        )
    return ray.is_initialized()


def _reset_gateway_ray_sync() -> None:
    import ray

    if ray.is_initialized():
        ray.shutdown()


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
            _ray_ready = _run_on_ray_thread(_init_gateway_ray_sync)
            return _ray_ready
        except Exception as exc:
            logger.warning("Gateway Ray initialization failed: %s", exc, exc_info=True)
            _ray_ready = False
            return False


def run_gateway_ray_operation(operation_name: str, fn: Callable[[], T]) -> T:
    """
    Run a Ray-backed gateway operation on the dedicated Ray thread.

    If the connection has gone stale, reset it and retry the operation once.
    """
    global _ray_ready

    if not init_gateway_ray():
        raise RuntimeError("Gateway Ray client is not initialized")

    try:
        return _run_on_ray_thread(fn)
    except Exception as exc:
        logger.warning(
            "Gateway Ray operation '%s' failed, attempting reconnect: %s",
            operation_name,
            exc,
            exc_info=True,
        )
        with _init_lock:
            try:
                _run_on_ray_thread(_reset_gateway_ray_sync)
            except Exception:
                pass
            _ray_ready = False
            if not init_gateway_ray():
                raise
        return _run_on_ray_thread(fn)


def list_agents_from_hub() -> list[dict]:
    """
    Return the live AgentHub listing using the dedicated Ray thread.
    """
    def _op() -> list[dict]:
        import ray
        from etl.agents.hub import get_hub

        hub = get_hub(create_if_missing=False)
        return ray.get(hub.list_agents.remote())

    return run_gateway_ray_operation("list_agents_from_hub", _op)


def call_agent_method(agent_name: str, method_name: str, *args: Any, **kwargs: Any) -> Any:
    """
    Invoke a Ray Serve deployment method through the dedicated Ray thread.
    """
    def _op() -> Any:
        import ray.serve as serve
        from etl.runtime import resolve_serve_response

        handle = serve.get_app_handle(agent_name)
        method = getattr(handle, method_name)
        remote = getattr(method, "remote")
        return resolve_serve_response(remote(*args, **kwargs))

    return run_gateway_ray_operation(f"call_agent_method:{agent_name}:{method_name}", _op)
