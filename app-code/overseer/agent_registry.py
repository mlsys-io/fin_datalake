"""
Agent registry utilities for the Overseer.

Supports resolving agent classes from:
1. Environment variable OVERSEER_AGENT_REGISTRY (JSON dict)
2. config.yaml "agents" mapping

Registry values accept:
- "module.path:ClassName"
- "relative/path/to/file.py:ClassName"
"""
from __future__ import annotations

import json
import os
import importlib
import importlib.util
from pathlib import Path
from typing import Any

from loguru import logger
from overseer.config import load_config

_ENV_REGISTRY = "OVERSEER_AGENT_REGISTRY"
_ENV_DEFAULT_AGENT = "OVERSEER_DEFAULT_AGENT"


class AgentResolutionError(RuntimeError):
    """Base class for agent import resolution failures."""


class UnrecoverableAgentResolutionError(AgentResolutionError):
    """Raised when a catalog recovery contract cannot resolve deterministically."""


def load_agent_registry(config_path: str | Path | None = None) -> dict[str, str]:
    """Load agent registry mapping {ClassName: import_target}."""
    raw = os.getenv(_ENV_REGISTRY)
    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                return {str(k): str(v) for k, v in data.items()}
        except json.JSONDecodeError:
            pass

    try:
        cfg = load_config(config_path)
    except Exception as exc:
        logger.warning("Failed to load Overseer agent registry from config: %s", exc)
        return {}

    agents = cfg.get("agents", {})
    if isinstance(agents, dict):
        return {str(k): str(v) for k, v in agents.items()}
    return {}


def get_default_agent_name(
    config_path: str | Path | None = None,
    fallback: str = "SentimentAgent",
) -> str:
    """Pick the default agent class name (env override > registry > fallback)."""
    env = os.getenv(_ENV_DEFAULT_AGENT)
    if env:
        return env
    registry = load_agent_registry(config_path)
    if registry:
        return sorted(registry.keys())[0]
    return fallback


def get_managed_agent_names(config_path: str | Path | None = None) -> set[str]:
    """Return the set of agent class names known to the registry."""
    return set(load_agent_registry(config_path).keys())


def resolve_agent_class(name: str, config_path: str | Path | None = None) -> Any:
    """Resolve an agent class by name using the registry mapping."""
    registry = load_agent_registry(config_path)
    target = registry.get(name)
    if not target:
        return None
    return _import_from_target(target)


def resolve_agent_import_target(target: str) -> Any:
    """Resolve a catalog class_path module import target.

    Catalog recovery targets are a durable contract, so they must be explicit
    module imports. File-path targets remain supported by the mutable registry,
    but are intentionally rejected here.
    """
    target = str(target or "").strip()
    if ":" not in target:
        raise UnrecoverableAgentResolutionError(
            "Missing or invalid catalog class_path; expected 'module.path:ClassName'."
        )

    module_path, class_name = target.split(":", 1)
    module_path = module_path.strip()
    class_name = class_name.strip()
    if not module_path or not class_name:
        raise UnrecoverableAgentResolutionError(
            "Missing or invalid catalog class_path; expected 'module.path:ClassName'."
        )

    if module_path.endswith(".py") or "/" in module_path or "\\" in module_path:
        raise UnrecoverableAgentResolutionError(
            f"Catalog class_path '{target}' must use a module path, not a file path."
        )

    try:
        resolved = _import_from_target(target)
    except Exception as exc:
        raise UnrecoverableAgentResolutionError(
            f"Catalog class_path '{target}' could not be imported: {exc}"
        ) from exc

    if resolved is None:
        raise UnrecoverableAgentResolutionError(
            f"Catalog class_path '{target}' did not resolve to an agent class."
        )
    return resolved


def _import_from_target(target: str) -> Any:
    """Import a class from a module path or file path target."""
    if ":" not in target:
        return None
    module_or_path, class_name = target.split(":", 1)
    module_or_path = module_or_path.strip()
    class_name = class_name.strip()
    if not module_or_path or not class_name:
        return None

    # File path target
    if module_or_path.endswith(".py") or "/" in module_or_path or "\\" in module_or_path:
        path = Path(module_or_path)
        if not path.is_absolute():
            base = Path(__file__).resolve().parents[1]  # app-code
            path = base / path
        if not path.exists():
            return None
        mod_name = f"overseer_agent_{path.stem}"
        spec = importlib.util.spec_from_file_location(mod_name, path)
        if spec is None or spec.loader is None:
            return None
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return getattr(mod, class_name, None)

    # Module path target
    mod = importlib.import_module(module_or_path)
    return getattr(mod, class_name, None)
