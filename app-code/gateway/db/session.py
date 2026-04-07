"""
Database session management.

Provides the PostgreSQL engine and an async session factory.
All auth-related tables (users, api_keys) live in gateway.db.
"""

import os

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# Primary auth database. Defaults to Postgres/TimescaleDB.
# Format: postgresql+asyncpg://user:pass@host:port/dbname
DATABASE_URL = os.environ.get(
    "GATEWAY_DATABASE_URL", 
    "postgresql+asyncpg://app:app@localhost:5432/app"
)

engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


_ENSURE_AGENT_DEFINITION_COLUMNS_SQL = [
    "ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS last_heartbeat_at TIMESTAMPTZ",
    "ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'unknown'",
    "ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS managed_by_overseer BOOLEAN NOT NULL DEFAULT TRUE",
    "ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS desired_status TEXT NOT NULL DEFAULT 'running'",
    "ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS observed_status TEXT NOT NULL DEFAULT 'unknown'",
    "ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS health_status TEXT NOT NULL DEFAULT 'unknown'",
    "ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS recovery_state TEXT NOT NULL DEFAULT 'idle'",
    "ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS last_reconciled_at TIMESTAMPTZ",
    "ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS last_failure_reason TEXT",
    "ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS last_action_type TEXT",
    "ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS reconcile_notes TEXT",
    "ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS runtime_source TEXT",
    "ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS runtime_namespace TEXT",
    "ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS route_prefix TEXT",
    "ALTER TABLE agent_definitions ADD COLUMN IF NOT EXISTS deployment_metadata TEXT NOT NULL DEFAULT '{}'",
]


class Base(DeclarativeBase):
    """Shared base class for all ORM models in the gateway DB."""
    pass


# ---------------------------------------------------------------------------
# FastAPI dependency
# ---------------------------------------------------------------------------

async def get_db() -> AsyncSession:
    """
    Yields an async DB session for use in FastAPI endpoints via Depends(get_db).
    """
    async with AsyncSessionLocal() as session:
        yield session


# ---------------------------------------------------------------------------
# Startup helper — called once from main.py
# ---------------------------------------------------------------------------

async def init_db():
    """
    Create all tables if they do not already exist.
    Also handles auto-provisioning of the first admin account if the DB is empty.
    """
    from sqlalchemy import select
    from gateway.db.models import UserORM, APIKeyORM
    from gateway.db import crud
    import secrets
    import string

    async with engine.begin() as conn:
        # Create tables (idempotent)
        await conn.run_sync(Base.metadata.create_all)
        for statement in _ENSURE_AGENT_DEFINITION_COLUMNS_SQL:
            await conn.exec_driver_sql(statement)
    
    # Check for empty users table to trigger auto-provisioning
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(UserORM))
        if not result.scalars().first():
            print("\n" + "="*60)
            print("FIRST RUN: Auto-provisioning Master Admin Account...")
            
            # Generate random password
            alphabet = string.ascii_letters + string.digits
            password = "".join(secrets.choice(alphabet) for _ in range(16))
            
            # Create user
            # Note: We use "Admin" role which corresponds to Permission.SYSTEM_ADMIN
            admin = await crud.create_user(
                db, 
                username="admin", 
                password=password, 
                role_names=["Admin"],
                email="admin@internal"
            )
            
            # Generate master API key
            api_key = await crud.create_api_key(db, "admin", description="Master Admin Key")
            
            print(f"  > Username: {admin.username}")
            print(f"  > Password: {password}")
            print(f"  > API Key:  {api_key}")
            print("\nIMPORTANT: Store these credentials safely! They are shown ONLY once.")
            print("="*60 + "\n")
