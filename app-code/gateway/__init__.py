"""
Gateway: Unified Interface Layer for the AI-Native Lakehouse.

Provides a protocol-agnostic, role-based access layer that routes
user requests (from REST, MCP, CLI, or SDK) to the correct internal
subsystem (Data, Compute, Agent, Broker).

Package Structure:
  gateway/
  ├── models/       - UserIntent, User, APIKey Pydantic/dataclass models
  ├── core/         - BaseAdapter, InterfaceRegistry (the routing engine)
  └── adapters/     - Concrete adapter implementations per Domain
"""

from gateway.core.registry import InterfaceRegistry
from gateway.core.adapters import BaseAdapter
from gateway.models.intent import UserIntent

__all__ = [
    "InterfaceRegistry",
    "BaseAdapter",
    "UserIntent",
]
