"""
InterfaceRegistry: The Core Router.

Routes an incoming UserIntent to the correct registered Adapter
based on intent.domain. Acts as the single chokepoint for all
privileged system operations.

Design:
    Registry is assembled once at application startup with all
    concrete adapters registered. It is then injected into each
    protocol translator (FastAPI, MCP, CLI).
"""

import logging
from typing import Any, Dict, Optional

from gateway.core.adapters import BaseAdapter, ActionNotFoundError, PermissionError
from gateway.models.intent import UserIntent
from gateway.models.user import User

logger = logging.getLogger(__name__)


class DomainNotFoundError(Exception):
    """Raised when no adapter is registered for the requested domain."""
    pass


class InterfaceRegistry:
    """
    The central routing engine for the Interface Layer.

    Maintains a map of {domain -> adapter} and routes UserIntents
    to the correct adapter for execution.

    Usage:
        registry = InterfaceRegistry()
        registry.register(DataAdapter())
        registry.register(ComputeAdapter())
        registry.register(AgentAdapter())
        registry.register(BrokerAdapter())

        result = registry.route(intent)
    """

    def __init__(self):
        self._adapters: Dict[str, BaseAdapter] = {}

    def register(self, adapter: BaseAdapter) -> "InterfaceRegistry":
        """
        Register a concrete adapter for its domain.

        Args:
            adapter: A BaseAdapter subclass instance.

        Returns:
            Self, for chaining: registry.register(A()).register(B())
        """
        domain = adapter.handles()
        if domain in self._adapters:
            logger.warning(
                "Overwriting existing adapter for domain '%s' with %s",
                domain,
                type(adapter).__name__,
            )
        self._adapters[domain] = adapter
        logger.info("Registered adapter '%s' for domain '%s'", type(adapter).__name__, domain)
        return self

    def route(self, user: User, intent: UserIntent) -> Any:
        """
        Route an intent to the appropriate adapter and execute it.

        Args:
            user:   The resolved User object from the auth layer.
            intent: The normalized UserIntent from any protocol translator.
        """
        logger.info(
            "Routing intent | user=%s | role=%s | domain=%s | action=%s",
            intent.user_id, intent.role, intent.domain, intent.action,
        )

        adapter = self._adapters.get(intent.domain)
        if adapter is None:
            raise DomainNotFoundError(
                f"No adapter registered for domain '{intent.domain}'. "
                f"Available domains: {list(self._adapters.keys())}"
            )

        try:
            result = adapter.execute(user, intent)
            logger.debug("Intent executed | domain=%s | action=%s", intent.domain, intent.action)
            return result
        except (PermissionError, ActionNotFoundError):
            raise  # Let the translator handle these cleanly
        except Exception as e:
            logger.exception("Unhandled error in adapter '%s': %s", type(adapter).__name__, e)
            raise

    def registered_domains(self) -> list[str]:
        """Return a list of all currently registered domain names."""
        return list(self._adapters.keys())

    def get_adapter(self, domain: str) -> Optional[BaseAdapter]:
        """Retrieve a specific adapter by domain name (useful for testing)."""
        return self._adapters.get(domain)


def build_default_registry() -> InterfaceRegistry:
    """
    Factory function that assembles the standard registry with all
    production adapters registered.

    Call this once at application startup and inject the result
    into your FastAPI app, MCP server, or GatewayClient.

    Returns:
        A fully populated InterfaceRegistry.
    """
    from gateway.adapters.data import DataAdapter
    from gateway.adapters.compute import ComputeAdapter
    from gateway.adapters.agent import AgentAdapter
    from gateway.adapters.broker import BrokerAdapter

    registry = InterfaceRegistry()
    registry.register(DataAdapter())
    registry.register(ComputeAdapter())
    registry.register(AgentAdapter())
    registry.register(BrokerAdapter())

    return registry
