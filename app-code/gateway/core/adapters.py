"""
BaseAdapter: The contract every Domain Adapter must fulfill.

Design Principle:
    An Adapter's only job is to translate a UserIntent into an action
    against a specific subsystem (Delta Lake, Prefect, Ray, etc.).
    It does NOT parse protocols (HTTP/MCP/CLI) — that is the Translator's job.

    Rule of thumb: 1 Adapter = 1 Domain (Subsystem).
    Actions within a domain are handled via internal method dispatch.
"""

from abc import ABC, abstractmethod
from typing import Any

from gateway.models.intent import UserIntent
from gateway.models.user import User
from gateway.core.rbac import Permission, rbac_provider


class PermissionError(Exception):
    """Raised when a user does not hold the required permission."""
    pass


class ActionNotFoundError(Exception):
    """Raised when an action is not supported by the adapter."""
    pass


class BaseAdapter(ABC):
    """
    Abstract base class for all Interface Layer adapters.

    Each concrete adapter handles one domain (e.g., 'data', 'compute').
    Within that domain, it routes `intent.action` to specific methods.

    Permission checks are done per-action via `_require_permission()`.
    """

    @abstractmethod
    def handles(self) -> str:
        """
        Returns the domain string this adapter is responsible for.
        """
        pass

    @abstractmethod
    async def execute(self, user: User, intent: UserIntent) -> Any:
        """
        Execute the intent against the adapter's subsystem.
        """
        pass

    def _require_permission(self, user: User, permission: Permission):
        """
        Assert a user holds a specific Permission before executing an action.

        Args:
            user:       The resolved User object.
            permission: The specific Permission enum value required.

        Raises:
            PermissionError: If the user does not hold the required permission.
        """
        if not rbac_provider.is_authorized(user.role_names, permission):
            raise PermissionError(
                f"User '{user.username}' lacks permission '{permission.value}'. "
            )
