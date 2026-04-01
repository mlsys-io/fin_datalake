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


def load_config(path: str | Path | None = None) -> dict[str, Any]:
    """Load the raw YAML config file."""
    config_path = Path(path) if path else _DEFAULT_CONFIG
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
        port = int(os.environ.get(f"{env_prefix}PORT", cfg.get("port", 8080)))
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
