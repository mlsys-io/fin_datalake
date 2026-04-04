"""
CRUD Operations for the Gateway Auth database.

Provides async helper functions used by the auth router and deps.py.
All functions take an AsyncSession as their first argument.
"""

import json
import secrets
import string
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from gateway.core.security import hash_password, verify_password
from gateway.db.models import APIKeyORM, AgentDefinitionORM, UserORM
from gateway.models.user import User
from gateway.core.rbac import rbac_provider


# ---------------------------------------------------------------------------
# User helpers
# ---------------------------------------------------------------------------

def _orm_to_user(row: UserORM) -> User:
    """Convert a UserORM row into the Pydantic User model."""
    role_names = json.loads(row.role_names)
    return User(
        username=row.username,
        hashed_password=row.hashed_password,
        role_names=role_names,
        is_active=row.is_active,
        email=row.email,
    )


async def get_user_by_username(db: AsyncSession, username: str) -> User | None:
    """Fetch a User by username. Returns None if not found or inactive."""
    result = await db.execute(
        select(UserORM).where(UserORM.username == username, UserORM.is_active == True)
    )
    row = result.scalars().first()
    return _orm_to_user(row) if row else None


async def authenticate_user(db: AsyncSession, username: str, password: str) -> User | None:
    """
    Verify a username + password combination.

    Returns the User on success, None on failure (wrong user or wrong password).
    Note: Always runs the password check even if the user is not found,
    to prevent timing attacks from revealing whether a user exists.
    """
    result = await db.execute(
        select(UserORM).where(UserORM.username == username, UserORM.is_active == True)
    )
    row = result.scalars().first()

    # Run bcrypt check regardless to prevent timing attacks using a valid dummy hash
    dummy_hash = "$2b$12$WG8v0b2N2wS9Lz7/y8/O0.e9c5WdJ1zW7/QO/0W.w6W.k8u.K8oOW"  # Hash of 'invalid'
    actual_hash = row.hashed_password if row else dummy_hash
    
    try:
        if not verify_password(password, actual_hash):
            return None
    except ValueError:
        # If the DB has an old plain-text password or corrupted hash, fail safely
        return None

    return _orm_to_user(row) if row else None


async def create_user(
    db: AsyncSession,
    username: str,
    password: str,
    role_names: list[str] | None = None,
    email: str | None = None,
) -> User:
    """
    Create a new user in the database.

    Raises:
        ValueError: If the username is already taken.
    """
    # Validate roles
    roles = role_names or ["Analyst"]
    try:
        rbac_provider.validate_roles(roles)
    except ValueError as e:
        raise ValueError(str(e))

    existing = await db.execute(select(UserORM).where(UserORM.username == username))
    if existing.scalars().first():
        raise ValueError(f"Username '{username}' is already taken.")

    orm_user = UserORM(
        username=username,
        hashed_password=hash_password(password),
        role_names=json.dumps(roles),
        email=email,
    )
    db.add(orm_user)
    await db.commit()
    await db.refresh(orm_user)
    return _orm_to_user(orm_user)


# ---------------------------------------------------------------------------
# API Key helpers
# ---------------------------------------------------------------------------

_KEY_ALPHABET = string.ascii_letters + string.digits


def _generate_raw_key() -> str:
    """Generate a cryptographically secure random API Key."""
    random_part = "".join(secrets.choice(_KEY_ALPHABET) for _ in range(32))
    return f"etl_sk_{random_part}"


async def create_api_key(
    db: AsyncSession,
    username: str,
    description: str = "Default API Key",
) -> str:
    """
    Generate a new API Key for a user.

    Args:
        db:          The database session.
        username:    The owner username (must already exist).
        description: Human-readable label for the key.

    Returns:
        The full raw key string (ONLY returned once — not stored).

    Raises:
        ValueError: If the user does not exist.
    """
    result = await db.execute(select(UserORM).where(UserORM.username == username))
    orm_user = result.scalars().first()
    if not orm_user:
        raise ValueError(f"User '{username}' not found.")

    raw_key = _generate_raw_key()
    key_prefix = raw_key[:16]  # "etl_sk_" + first 9 chars

    orm_key = APIKeyORM(
        user_id=orm_user.id,
        key_prefix=key_prefix,
        key_hash=hash_password(raw_key),
        description=description,
    )
    db.add(orm_key)
    await db.commit()
    return raw_key  # Only time it is ever returned in plain text


async def resolve_api_key(db: AsyncSession, raw_key: str) -> User | None:
    """
    Validate a raw API Key and return the owning User.

    Fetches all active keys that share the same prefix, then verifies
    the bcrypt hash to find a valid match.

    Returns None if the key is invalid or revoked.
    """
    if not raw_key.startswith("etl_sk_"):
        return None

    prefix = raw_key[:16]
    result = await db.execute(
        select(APIKeyORM)
        .where(APIKeyORM.key_prefix == prefix, APIKeyORM.is_active == True)
        .join(APIKeyORM.user)
        .where(UserORM.is_active == True)
    )
    rows = result.scalars().all()

    for key_row in rows:
        if verify_password(raw_key, key_row.key_hash):
            # Found the matching key — fetch the full user
            user_result = await db.execute(
                select(UserORM).where(UserORM.id == key_row.user_id)
            )
            orm_user = user_result.scalars().first()
            return _orm_to_user(orm_user) if orm_user else None

    return None  # No matching key found


async def list_api_keys(db: AsyncSession, username: str) -> list[dict]:
    """List a user's active API keys (prefixes only — never hashes)."""
    result = await db.execute(
        select(APIKeyORM)
        .join(APIKeyORM.user)
        .where(UserORM.username == username, APIKeyORM.is_active == True)
    )
    return [
        {
            "id": row.id,
            "prefix": row.key_prefix,
            "description": row.description,
            "created_at": row.created_at.isoformat(),
        }
        for row in result.scalars().all()
    ]


async def revoke_api_key(db: AsyncSession, key_id: str, username: str) -> bool:
    """
    Revoke (soft-delete) an API Key by its ID.
    Ensures the key belongs to the requesting user.

    Returns True if revoked, False if not found.
    """
    result = await db.execute(
        select(APIKeyORM)
        .join(APIKeyORM.user)
        .where(APIKeyORM.id == key_id, UserORM.username == username)
    )
    row = result.scalars().first()
    if not row:
        return False

    row.is_active = False
    await db.commit()
    return True


# ---------------------------------------------------------------------------
# Agent Catalog helpers
# ---------------------------------------------------------------------------

def _parse_json_text(value: str | None, default):
    if not value:
        return default
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return default


def _agent_definition_to_dict(row: AgentDefinitionORM) -> dict:
    return {
        "name": row.name,
        "capabilities": _parse_json_text(row.capabilities, []),
        "capability_specs": _parse_json_text(row.capability_specs, []),
        "metadata": _parse_json_text(row.metadata_json, {}),
        "deployment_metadata": _parse_json_text(row.deployment_metadata_json, {}),
        "registered_at": row.registered_at.isoformat() if row.registered_at else None,
        "last_seen_at": row.last_seen_at.isoformat() if row.last_seen_at else None,
        "last_heartbeat_at": row.last_heartbeat_at.isoformat() if row.last_heartbeat_at else None,
        "status": row.status,
        "runtime_source": row.runtime_source,
        "runtime_namespace": row.runtime_namespace,
        "route_prefix": row.route_prefix,
        "alive": row.status == "alive",
        "source": "catalog",
    }


async def upsert_agent_definition(
    db: AsyncSession,
    agent: dict,
    *,
    mark_seen: bool = True,
) -> dict:
    """
    Insert or update a persistent agent catalog entry from a live/runtime payload.
    """
    agent_name = str(agent.get("name") or "").strip()
    if not agent_name:
        raise ValueError("Agent definition requires a non-empty 'name'.")

    result = await db.execute(
        select(AgentDefinitionORM).where(AgentDefinitionORM.name == agent_name)
    )
    row = result.scalars().first()
    now = datetime.now(timezone.utc)

    registered_at = agent.get("registered_at")
    if isinstance(registered_at, str):
        try:
            registered_at = datetime.fromisoformat(registered_at)
        except ValueError:
            registered_at = None
    elif not isinstance(registered_at, datetime):
        registered_at = None

    if row is None:
        row = AgentDefinitionORM(name=agent_name)
        db.add(row)

    row.capabilities = json.dumps(agent.get("capabilities") or [])
    row.capability_specs = json.dumps(agent.get("capability_specs") or [])
    row.metadata_json = json.dumps(agent.get("metadata") or {})
    row.deployment_metadata_json = json.dumps(agent.get("deployment_metadata") or {})
    row.runtime_source = agent.get("runtime_source") or row.runtime_source or "ray-serve"
    row.runtime_namespace = agent.get("runtime_namespace") or row.runtime_namespace
    row.route_prefix = agent.get("route_prefix") or row.route_prefix
    row.status = str(agent.get("status") or row.status or "unknown")
    if registered_at is not None:
        row.registered_at = registered_at
    if mark_seen:
        row.last_seen_at = now
        row.last_heartbeat_at = now

    await db.commit()
    await db.refresh(row)
    return _agent_definition_to_dict(row)


async def mark_agent_status(
    db: AsyncSession,
    name: str,
    *,
    status: str,
    mark_seen: bool = False,
    heartbeat: bool = False,
) -> dict | None:
    """
    Update runtime status metadata for a known agent catalog entry.
    """
    result = await db.execute(
        select(AgentDefinitionORM).where(AgentDefinitionORM.name == name)
    )
    row = result.scalars().first()
    if row is None:
        return None

    now = datetime.now(timezone.utc)
    row.status = status
    if mark_seen:
        row.last_seen_at = now
    if heartbeat:
        row.last_heartbeat_at = now

    await db.commit()
    await db.refresh(row)
    return _agent_definition_to_dict(row)


async def mark_stale_agents(
    db: AsyncSession,
    *,
    stale_before: datetime,
    from_statuses: list[str] | None = None,
    stale_status: str = "stale",
) -> int:
    """
    Mark agents as stale when they have not heartbeated recently.
    """
    stmt = select(AgentDefinitionORM).where(
        AgentDefinitionORM.last_heartbeat_at.is_not(None),
        AgentDefinitionORM.last_heartbeat_at < stale_before,
    )
    if from_statuses:
        stmt = stmt.where(AgentDefinitionORM.status.in_(from_statuses))

    result = await db.execute(stmt)
    rows = result.scalars().all()
    for row in rows:
        row.status = stale_status

    await db.commit()
    return len(rows)


async def list_agent_definitions(db: AsyncSession, *, enabled_only: bool = True) -> list[dict]:
    """
    Return the persistent agent catalog from the database.
    """
    stmt = select(AgentDefinitionORM).order_by(AgentDefinitionORM.name.asc())
    if enabled_only:
        stmt = stmt.where(AgentDefinitionORM.is_enabled == True)

    result = await db.execute(stmt)
    return [_agent_definition_to_dict(row) for row in result.scalars().all()]
