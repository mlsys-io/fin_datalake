"""Lazy exports for the agents package.

Keep package import side effects light so modules like ``etl.agents.hub`` can
be used from services that do not install every optional dependency.
"""

from importlib import import_module
from typing import Any

__all__ = [
    "BaseAgent",
    "GeminiAgent",
    "LangChainAgent",
    "SentimentAgent",
    "DeploymentSpec",
    "resolve_agent_class",
    "baseline_fleet_specs",
    "deploy_agent",
    "deploy_fleet",
    "deploy_baseline_fleet",
    "delete_agent",
    "delete_agent_deployment",
    "delete_fleet",
    "delete_baseline_fleet",
    "list_fleet_state",
    "TimescaleTool",
    "MilvusTool",
    "AgentHub",
    "get_hub",
    "ContextStore",
    "get_context",
]

_EXPORTS = {
    "BaseAgent": (".base", "BaseAgent"),
    "GeminiAgent": (".gemini_agent", "GeminiAgent"),
    "LangChainAgent": (".langchain_adapter", "LangChainAgent"),
    "SentimentAgent": (".sentiment_agent", "SentimentAgent"),
    "DeploymentSpec": (".manager", "DeploymentSpec"),
    "resolve_agent_class": (".manager", "resolve_agent_class"),
    "baseline_fleet_specs": (".manager", "baseline_fleet_specs"),
    "deploy_agent": (".manager", "deploy_agent"),
    "deploy_fleet": (".manager", "deploy_fleet"),
    "deploy_baseline_fleet": (".manager", "deploy_baseline_fleet"),
    "delete_agent": (".manager", "delete_agent"),
    "delete_agent_deployment": (".manager", "delete_agent_deployment"),
    "delete_fleet": (".manager", "delete_fleet"),
    "delete_baseline_fleet": (".manager", "delete_baseline_fleet"),
    "list_fleet_state": (".manager", "list_fleet_state"),
    "TimescaleTool": (".tools", "TimescaleTool"),
    "MilvusTool": (".tools", "MilvusTool"),
    "AgentHub": (".hub", "AgentHub"),
    "get_hub": (".hub", "get_hub"),
    "ContextStore": (".context", "ContextStore"),
    "get_context": (".context", "get_context"),
}


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module_name, attr_name = _EXPORTS[name]
    module = import_module(module_name, __name__)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
