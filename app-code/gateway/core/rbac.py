"""
Dynamic RBAC Provider for the Gateway.

Standardizes Identity rules (Roles and Policies) and fetches them dynamically
from the central store (e.g., Redis). Mirrors the Overseer's autonomic policy loading.
"""

import json
import logging
from enum import Enum
from typing import Dict, List, Optional
from pydantic import BaseModel, Field

from gateway.core.redis import get_redis_client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 1. RBAC Models
# ---------------------------------------------------------------------------

class Permission(str, Enum):
    """Atomic units of system interaction."""
    # Data Access
    DATA_READ = "data:read"
    DATA_WRITE = "data:write"
    DATA_PREVIEW = "data:preview"

    # Compute Access
    COMPUTE_READ = "compute:read"
    COMPUTE_WRITE = "compute:write"

    # Agent Interaction
    AGENT_READ = "agent:read"
    AGENT_INTERACT = "agent:interact"
    AGENT_BROADCAST = "agent:broadcast"

    # Broker/Connection Control
    BROKER_READ = "broker:read"
    BROKER_VEND = "broker:vend"

    # System/Observability
    SYSTEM_READ = "system:read"
    SYSTEM_ADMIN = "system:admin"


class Policy(BaseModel):
    """A statement allowing a list of permissions."""
    name: str
    description: str = ""
    permissions: List[Permission] = Field(default_factory=list)

    def has_permission(self, permission: Permission) -> bool:
        return permission in self.permissions


class Role(BaseModel):
    """A collection of policies that can be assigned to a User."""
    name: str
    description: str = ""
    policies: List[Policy] = Field(default_factory=list)

    def has_permission(self, permission: Permission) -> bool:
        return any(p.has_permission(permission) for p in self.policies)


# ---------------------------------------------------------------------------
# 2. Defaults
# ---------------------------------------------------------------------------

DEFAULT_POLICIES = {
    "DataReadOnly": Policy(
        name="DataReadOnly",
        description="Read-only access to data queries.",
        permissions=[Permission.DATA_READ, Permission.DATA_PREVIEW]
    ),
    "AdminFullAccess": Policy(
        name="AdminFullAccess",
        description="Full access to all domains.",
        permissions=list(Permission)
    ),
    "AgentOperator": Policy(
        name="AgentOperator",
        description="Access to interact with agents.",
        permissions=[Permission.AGENT_READ, Permission.AGENT_INTERACT]
    )
}

DEFAULT_ROLES = {
    "Analyst": Role(
        name="Analyst",
        description="Default role for new users.",
        policies=[DEFAULT_POLICIES["DataReadOnly"], DEFAULT_POLICIES["AgentOperator"]]
    ),
    "Admin": Role(
        name="Admin",
        description="System administrator.",
        policies=[DEFAULT_POLICIES["AdminFullAccess"]]
    )
}


# ---------------------------------------------------------------------------
# 3. RBAC Provider
# ---------------------------------------------------------------------------

class RBACProvider:
    """
    Singleton provider that handles role loading and permission checks.
    """
    _instance: Optional["RBACProvider"] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(RBACProvider, cls).__new__(cls)
            cls._instance._roles = {}
        return cls._instance

    def __init__(self):
        # Ensure _roles exists even if __new__ path was skipped somehow
        if not hasattr(self, '_roles'):
            self._roles: Dict[str, Role] = {}

    async def load(self) -> Dict[str, Role]:
        """Load roles from Redis or defaults."""
        r = get_redis_client()
        roles = {}

        if r:
            try:
                # Expected format: { "role_name": { "name": "...", "policies": [...] } }
                raw = await r.get("gateway:rbac:roles")
                if raw:
                    data = json.loads(raw)
                    for name, r_data in data.items():
                        roles[name] = Role.model_validate(r_data)
                    logger.info("Loaded %d roles from Redis.", len(roles))
            except Exception as e:
                logger.error("Failed to load roles from Redis: %s", e)

        if not roles:
            logger.info("Falling back to default RBAC roles.")
            roles = DEFAULT_ROLES

        self._roles = roles
        return roles

    def is_authorized(self, role_names: List[str], permission: Permission) -> bool:
        """Central authorization check."""
        for name in role_names:
            role = self._roles.get(name)
            if role and role.has_permission(permission):
                return True
        return False

    def validate_roles(self, role_names: List[str]) -> List[str]:
        """Validated role names against known roles."""
        for name in role_names:
            if name not in self._roles:
                raise ValueError(f"Unknown role: '{name}'. Available: {list(self._roles.keys())}")
        return role_names

    def get_permissions(self, role_names: List[str]) -> List[Permission]:
        """Return deduplicated list of all permissions granted by these roles."""
        perms = set()
        for name in role_names:
            role = self._roles.get(name)
            if role:
                for policy in role.policies:
                    perms.update(policy.permissions)
        return sorted(list(perms))


# Global singleton — exported as both names for convenience.
provider = RBACProvider()
rbac_provider = provider  # Alias used by adapters, crud, and auth

async def load_roles():
    """App-wide helper to synchronize roles."""
    return await rbac_provider.load()
