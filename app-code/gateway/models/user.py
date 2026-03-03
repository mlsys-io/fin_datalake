"""
User & Authentication Models — AWS IAM-Style Permission System.

Design:
    Permission  — A single atomic capability string (e.g., "data:read").
    Policy      — A named, reusable bundle of Permissions.
    Role        — A named set of Policies that a User can assume.
    User        — An entity with one or more Roles assigned.

Why this model vs simple role hierarchy (Admin > Engineer > Analyst)?
    Flexibility: A user can be a "DataReader" + "AgentOperator" without
    needing full "Engineer" privileges. Fine-grained access matches
    real-world enterprise setups and is much closer to how production
    IAM systems (AWS, GCP) work.

Permission Naming Convention:
    <domain>:<action_scope>
    e.g.  data:read, compute:write, agent:broadcast, system:admin
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional


# ---------------------------------------------------------------------------
# 1. Permissions — Atomic capabilities
# ---------------------------------------------------------------------------

class Permission(str, Enum):
    """
    Fine-grained permission strings scoped to each Gateway domain.

    Naming convention: <domain>:<scope>
    """
    # Data Domain
    DATA_READ = "data:read"          # run_sql, list_tables, get_schema, preview
    DATA_WRITE = "data:write"        # write back to Delta (future)

    # Compute Domain
    COMPUTE_READ = "compute:read"    # list_jobs, get_status
    COMPUTE_WRITE = "compute:write"  # submit_job, cancel_job

    # Agent Domain
    AGENT_READ = "agent:read"        # list agents
    AGENT_INTERACT = "agent:interact"  # chat (ask/response)
    AGENT_BROADCAST = "agent:broadcast"  # publish to MessageBus (system-wide impact)

    # Broker Domain (Credential Vending)
    BROKER_READ = "broker:read"      # list_connections (metadata only)
    BROKER_VEND = "broker:vend"      # get_s3_creds, get_psql_string (sensitive)

    # System
    SYSTEM_ADMIN = "system:admin"    # User management, provisioning


# ---------------------------------------------------------------------------
# 2. Built-in Policies — Reusable permission bundles
# ---------------------------------------------------------------------------

@dataclass
class Policy:
    """A named, reusable bundle of permissions."""
    name: str
    description: str
    permissions: List[Permission] = field(default_factory=list)


# Pre-defined system policies (inspired by AWS managed policies)
POLICY_DATA_READ_ONLY = Policy(
    name="DataReadOnly",
    description="Read-only access to data queries and schema inspection.",
    permissions=[Permission.DATA_READ],
)

POLICY_DATA_ENGINEER = Policy(
    name="DataEngineerAccess",
    description="Full data read/write and job management.",
    permissions=[
        Permission.DATA_READ,
        Permission.DATA_WRITE,
        Permission.COMPUTE_READ,
        Permission.COMPUTE_WRITE,
    ],
)

POLICY_AGENT_OPERATOR = Policy(
    name="AgentOperatorAccess",
    description="Ability to list, chat, and broadcast to agents.",
    permissions=[
        Permission.AGENT_READ,
        Permission.AGENT_INTERACT,
        Permission.AGENT_BROADCAST,
    ],
)

POLICY_BROKER_ACCESS = Policy(
    name="BrokerAccess",
    description="Access to list and obtain direct connection credentials.",
    permissions=[
        Permission.BROKER_READ,
        Permission.BROKER_VEND,
    ],
)

POLICY_ADMIN = Policy(
    name="AdministratorAccess",
    description="Full access to all gateway domains and system administration.",
    permissions=list(Permission),  # All permissions
)


# ---------------------------------------------------------------------------
# 3. Roles — Named sets of policies (users assume roles)
# ---------------------------------------------------------------------------

@dataclass
class Role:
    """
    A named role that groups multiple Policies together.
    Users are assigned one or more Roles.

    Built-in roles mirror common use cases.
    """
    name: str
    description: str
    policies: List[Policy] = field(default_factory=list)

    @property
    def permissions(self) -> List[Permission]:
        """Flatten all policies into a unique set of permissions."""
        all_perms: List[Permission] = []
        for policy in self.policies:
            for perm in policy.permissions:
                if perm not in all_perms:
                    all_perms.append(perm)
        return all_perms

    def has_permission(self, permission: Permission) -> bool:
        """Check if this role grants a specific permission."""
        return permission in self.permissions


# Built-in system roles
ROLE_ANALYST = Role(
    name="Analyst",
    description="Read-only access to data and ability to chat with agents.",
    policies=[POLICY_DATA_READ_ONLY, Policy(
        name="AnalystAgent",
        description="Read and interact with agents.",
        permissions=[Permission.AGENT_READ, Permission.AGENT_INTERACT, Permission.COMPUTE_READ, Permission.BROKER_READ]
    )],
)

ROLE_DATA_ENGINEER = Role(
    name="DataEngineer",
    description="Full data and compute access; ability to manage pipelines and agents.",
    policies=[POLICY_DATA_ENGINEER, POLICY_AGENT_OPERATOR, POLICY_BROKER_ACCESS],
)

ROLE_ADMIN = Role(
    name="Admin",
    description="Full unrestricted access to all gateway capabilities.",
    policies=[POLICY_ADMIN],
)

# Registry of built-in roles for lookup
BUILTIN_ROLES = {r.name: r for r in [ROLE_ANALYST, ROLE_DATA_ENGINEER, ROLE_ADMIN]}


# ---------------------------------------------------------------------------
# 4. User — An entity with assigned roles
# ---------------------------------------------------------------------------

@dataclass
class User:
    """
    Represents an authenticated user in the Gateway system.

    A User has one or more Roles. Permissions are derived from those Roles
    at runtime (like AWS IAM policy evaluation).

    Example:
        # Assign roles on user creation
        user = User(username="alice", roles=["Analyst", "AgentOperatorAccess"])

        # Check before executing an action
        user.has_permission(Permission.AGENT_INTERACT)  # True
        user.has_permission(Permission.BROKER_VEND)     # False
    """
    username: str
    hashed_password: str
    role_names: List[str] = field(default_factory=lambda: ["Analyst"])  # Default: least privilege
    is_active: bool = True
    email: Optional[str] = None

    @property
    def roles(self) -> List[Role]:
        """Resolve role names to Role objects."""
        return [BUILTIN_ROLES[r] for r in self.role_names if r in BUILTIN_ROLES]

    def has_permission(self, permission: Permission) -> bool:
        """
        Check if this user has a specific permission via any of their roles.

        Equivalent to AWS IAM policy evaluation: if any attached policy
        grants the permission, access is allowed (no explicit deny here).
        """
        return any(role.has_permission(permission) for role in self.roles)

    def all_permissions(self) -> List[Permission]:
        """Return the complete, deduplicated list of effective permissions."""
        seen = set()
        result = []
        for role in self.roles:
            for perm in role.permissions:
                if perm not in seen:
                    seen.add(perm)
                    result.append(perm)
        return result


@dataclass
class APIKey:
    """
    A programmatic API key tied to a User.
    Used for CLI and SDK authentication (Bearer token).

    The key_prefix (etl_sk_) is stored in plain text for identification.
    The key_hash is stored for secure validation (bcrypt).
    """
    key_prefix: str        # First 16 chars, e.g. "etl_sk_abc123" (safe to display)
    key_hash: str          # bcrypt hash of the full key (for validation)
    user_id: str           # Username of the owner
    description: str = "Default API Key"
    is_active: bool = True
