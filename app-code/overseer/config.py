"""
Overseer Configuration Loader.

Reads service endpoints from config.yaml and constructs ServiceEndpoint objects.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

from overseer.models import ServiceEndpoint

_DEFAULT_CONFIG = Path(__file__).parent / "config.yaml"
_ENV_CONFIG_PATH = "OVERSEER_CONFIG_PATH"


def _coerce_port(raw: object, *, name: str, fallback: int) -> int:
    if raw in (None, ""):
        return fallback
    try:
        port = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a valid integer port, got {raw!r}.") from exc
    if not 1 <= port <= 65535:
        raise ValueError(f"{name} must be between 1 and 65535, got {port}.")
    return port


def resolve_config_path(path: str | Path | None = None) -> Path:
    """Resolve the active Overseer config path."""
    if path:
        return Path(path)
    env_path = os.environ.get(_ENV_CONFIG_PATH)
    if env_path:
        return Path(env_path)
    return _DEFAULT_CONFIG


def load_config(path: str | Path | None = None) -> dict[str, Any]:
    """Load the raw YAML config file."""
    config_path = resolve_config_path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Overseer config not found: {config_path}")
    with open(config_path, "r") as f:
        return yaml.safe_load(f) or {}


def load_endpoints(path: str | Path | None = None) -> list[ServiceEndpoint]:
    """Parse config.yaml into a list of ServiceEndpoint objects."""
    raw = load_config(path)
    services = raw.get("services", {})
    endpoints = []
    for name, cfg in services.items():
        # Allow env-var overrides: OVERSEER_RAY_HOST, OVERSEER_RAY_PORT, etc.
        env_prefix = f"OVERSEER_{name.upper()}_"
        host = os.environ.get(f"{env_prefix}HOST", cfg.get("host", "localhost"))
        port = _coerce_port(os.environ.get(f"{env_prefix}PORT", cfg.get("port", 8080)), name=f"{env_prefix}PORT", fallback=8080)
        protocol = os.environ.get(f"{env_prefix}PROTOCOL", cfg.get("protocol", "http"))

        endpoints.append(ServiceEndpoint(
            name=name,
            host=host,
            port=port,
            protocol=protocol,
            health_path=cfg.get("health_path"),
            extra=cfg.get("extra", {}),
        ))
    return endpoints
