"""
Runtime helpers for cluster-backed execution.
"""

from __future__ import annotations

import os
import asyncio
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import Any


DEFAULT_WORKING_DIR_EXCLUDES = [
    ".venv",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "frontend/node_modules",
    "frontend/dist",
]


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _default_working_dir() -> Path:
    return Path(__file__).resolve().parents[1]


def resolve_working_dir(working_dir: str | os.PathLike[str] | None = None) -> str | None:
    """
    Resolve the working directory used for Ray runtime_env code shipping.

    Resolution order:
    1. Explicit function argument
    2. ``RAY_WORKING_DIR`` environment variable
    3. Repo default: ``app-code/``

    Set ``RAY_WORKING_DIR`` to an empty string to disable working_dir shipping.
    Relative paths are resolved against the repository root.
    """
    raw = working_dir
    if raw is None:
        env_value = os.environ.get("RAY_WORKING_DIR")
        if env_value is not None:
            raw = env_value
        else:
            raw = _default_working_dir()

    if raw is None:
        return None

    raw_str = str(raw).strip()
    if not raw_str:
        return None

    path = Path(raw_str)
    if not path.is_absolute():
        path = _repo_root() / path
    return str(path.resolve())


def build_runtime_env(working_dir: str | os.PathLike[str] | None = None) -> dict[str, Any] | None:
    """
    Build a runtime_env payload for Ray using a configurable working_dir.
    """
    resolved = resolve_working_dir(working_dir)
    if not resolved:
        return None

    return {
        "working_dir": resolved,
        "excludes": list(DEFAULT_WORKING_DIR_EXCLUDES),
    }


def resolve_ray_namespace(namespace: str | None = None) -> str | None:
    """
    Resolve the Ray namespace used by clients, actors, and gateway adapters.

    Resolution order:
    1. Explicit function argument
    2. ``RAY_NAMESPACE`` environment variable / config
    3. Default: ``serve``
    """
    if namespace is not None:
        value = str(namespace).strip()
        return value or None

    from etl.config import config

    value = str(config.RAY_NAMESPACE or "").strip()
    return value or None


def ensure_ray(
    address: str | None = None,
    *,
    working_dir: str | os.PathLike[str] | None = None,
    namespace: str | None = None,
    **init_kwargs: Any,
):
    """
    Initialize Ray once using the configured cluster address.

    Callers can override the address or pass additional ``ray.init`` kwargs.
    If Ray is already initialized, the active context is returned unchanged.
    """
    import ray

    if ray.is_initialized():
        return ray

    from etl.config import config

    init_kwargs.setdefault("ignore_reinit_error", True)
    init_kwargs.setdefault("runtime_env", build_runtime_env(working_dir))
    resolved_namespace = resolve_ray_namespace(namespace)
    if resolved_namespace is not None:
        init_kwargs.setdefault("namespace", resolved_namespace)
    ray.init(address=address or config.RAY_ADDRESS, **init_kwargs)
    return ray


def resolve_serve_response(response: Any) -> Any:
    """
    Resolve a Ray Serve DeploymentResponse or fall back to ``ray.get``.

    Ray Serve handle calls return DeploymentResponse objects, not plain
    ObjectRefs. Older actor-style paths may still return ObjectRefs, so this
    helper accepts both.
    """
    if hasattr(response, "result") and callable(response.result):
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return response.result()

        # Ray Serve forbids calling sync response resolution on the active
        # event-loop thread. Offload that blocking wait to a helper thread so
        # sync agent code can still delegate directly to other Serve apps.
        with ThreadPoolExecutor(max_workers=1) as executor:
            return executor.submit(response.result).result()

    import ray

    return ray.get(response)
