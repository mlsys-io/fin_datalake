"""
SQLAlchemy ORM Models for the Gateway Auth database.

Tables:
  - users:    Human and service accounts (username, hashed_password, roles).
  - api_keys: Long-lived machine credentials tied to a user.
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, String, Text
from sqlalchemy.orm import relationship

from gateway.db.session import Base


def _uuid() -> str:
    return str(uuid.uuid4())


def _now() -> datetime:
    return datetime.now(timezone.utc)


class UserORM(Base):
    """
    Persisted user record. Maps to the Pydantic `User` and `UserRole` models.

    columns:
        id              — UUID primary key.
        username        — Unique login name (e.g., "garret").
        hashed_password — bcrypt hash of the user's password.
        role_names      — JSON-serialised list of role names (e.g., '["DataEngineer"]').
        email           — Optional contact email.
        is_active       — Soft-delete flag; inactive users cannot log in.
        created_at      — UTC timestamp of account creation.
    """
    __tablename__ = "users"

    id = Column(String, primary_key=True, default=_uuid)
    username = Column(String, unique=True, nullable=False, index=True)
    hashed_password = Column(String, nullable=False)
    role_names = Column(Text, nullable=False, default='["Analyst"]')  # JSON string
    email = Column(String, nullable=True)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), default=_now)

    api_keys = relationship("APIKeyORM", back_populates="user", cascade="all, delete-orphan")


class APIKeyORM(Base):
    """
    A long-lived programmatic token tied to a UserORM.

    The full raw key is NEVER stored. Only:
    - key_prefix (first 16 chars, safe to display for identification)
    - key_hash   (bcrypt hash of the full key, for secure comparison)

    columns:
        id          — UUID primary key.
        user_id     — FK to users.id.
        key_prefix  — Displayable prefix e.g. "etl_sk_a1b2c3d4".
        key_hash    — bcrypt hash of the full raw key.
        description — Human-readable label set by the user.
        is_active   — Allows per-key revocation without deleting.
        created_at  — UTC timestamp of key creation.
    """
    __tablename__ = "api_keys"

    id = Column(String, primary_key=True, default=_uuid)
    user_id = Column(String, ForeignKey("users.id"), nullable=False)
    key_prefix = Column(String, nullable=False, index=True)
    key_hash = Column(String, nullable=False)
    description = Column(String, nullable=False, default="Default API Key")
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), default=_now)

    user = relationship("UserORM", back_populates="api_keys")
