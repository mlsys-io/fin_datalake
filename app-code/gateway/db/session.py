"""
Database session management.

Provides the SQLite engine and an async session factory.
All auth-related tables (users, api_keys) live in gateway.db.

The database file lives at GATEWAY_DB_PATH (default: ./gateway.db).
"""

import os

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_DB_PATH = os.environ.get("GATEWAY_DB_PATH", "./gateway.db")
DATABASE_URL = f"sqlite+aiosqlite:///{_DB_PATH}"

engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    """Shared base class for all ORM models in the gateway DB."""
    pass


# ---------------------------------------------------------------------------
# FastAPI dependency
# ---------------------------------------------------------------------------

async def get_db() -> AsyncSession:
    """
    Yields an async DB session for use in FastAPI endpoints via Depends(get_db).

    Usage in a route:
        async def my_route(db: AsyncSession = Depends(get_db)):
            ...
    """
    async with AsyncSessionLocal() as session:
        yield session


# ---------------------------------------------------------------------------
# Startup helper — called once from main.py
# ---------------------------------------------------------------------------

async def init_db():
    """
    Create all tables if they do not already exist.
    Call this in the FastAPI `startup` event handler.
    """
    from gateway.db.models import UserORM, APIKeyORM  # noqa: F401 — ensure models are registered
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
