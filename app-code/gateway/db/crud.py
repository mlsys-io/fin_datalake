"""
CRUD Operations for the Gateway Auth database.

Provides async helper functions used by the auth router and deps.py.
All functions take an AsyncSession as their first argument.
"""

import json
import secrets
import string

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from gateway.core.config import hash_password, verify_password
from gateway.db.models import APIKeyORM, UserORM
from gateway.models.user import User, BUILTIN_ROLES


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

    # Run bcrypt check regardless to prevent timing attacks
    dummy_hash = "$2b$12$invalidhashpadding000000000000000000000000000000000000"
    actual_hash = row.hashed_password if row else dummy_hash
    if not verify_password(password, actual_hash):
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
    for r in roles:
        if r not in BUILTIN_ROLES:
            raise ValueError(f"Unknown role '{r}'. Valid roles: {list(BUILTIN_ROLES.keys())}")

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
