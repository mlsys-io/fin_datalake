"""
Runtime helpers for cluster-backed execution.
"""

from __future__ import annotations

from typing import Any


def ensure_ray(address: str | None = None, **init_kwargs: Any):
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
    ray.init(address=address or config.RAY_ADDRESS, **init_kwargs)
    return ray
