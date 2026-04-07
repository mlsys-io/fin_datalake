"""
FastAPI Shared Dependencies (Dependency Injection layer).

Contains FastAPI `Depends(...)` callables used by route handlers.
Centralizes auth resolution and registry access, keeping routers thin.

Auth Flow:
  1. Request arrives with either:
     a. Authorization: Bearer <token>  → resolved as API Key or JWT  (CLI / SDK)
     b. Cookie: gateway_token=<jwt>   → resolved as JWT              (Browser)
  2. Token is decoded / DB lookup performed.
  3. Resolved User is injected into the route handler.
"""

import os
from datetime import datetime, timezone, timedelta

from fastapi import Cookie, Depends, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from sqlalchemy.ext.asyncio import AsyncSession

from gateway.core.config import JWT_SECRET_KEY, JWT_ALGORITHM, JWT_EXPIRE_MINUTES
from gateway.core.security import create_jwt, decode_jwt
from gateway.core.registry import InterfaceRegistry
from gateway.db.session import get_db
from gateway.db import crud
from gateway.models.user import User
from gateway.api.errors import api_error

# ---------------------------------------------------------------------------
# 1. Registry Dependency
# ---------------------------------------------------------------------------

def get_registry(request: Request) -> InterfaceRegistry:
    """Return the singleton InterfaceRegistry from app state (set at startup)."""
    registry: InterfaceRegistry = getattr(request.app.state, "registry", None)
    if registry is None:
        raise api_error(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Gateway registry is not initialized.",
            code="registry_unavailable",
        )
    return registry


# JWT helpers are now in gateway.core.security


# ---------------------------------------------------------------------------
# 3. Auth Dependency
# ---------------------------------------------------------------------------

_bearer_scheme = HTTPBearer(auto_error=False)


async def get_current_user(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
    gateway_token: str | None = Cookie(default=None),
    db: AsyncSession = Depends(get_db),
) -> User:
    """
    Resolve the current authenticated User from:
    - Path A: Bearer token in Authorization header
        - If it starts with 'etl_sk_' → treat as API Key, look up in DB.
        - Otherwise → decode as JWT, look up username.
    - Path B: HttpOnly cookie 'gateway_token' → decode as JWT.
    - Path C: System Service Account → If token matches `GATEWAY_INTERNAL_TOKEN`.

    Raises HTTP 401 if no valid credential is found.
    """
    user: User | None = None

    # --- Path A: Bearer Token ---
    if credentials and credentials.credentials:
        token = credentials.credentials

        # A.1: Internal Service Account (M2M) Fast-Path
        internal_token = os.environ.get("GATEWAY_INTERNAL_TOKEN")
        if internal_token and token == internal_token:
            return User(
                username="system:overseer",
                hashed_password="[SERVICE_ACCOUNT]",
                role_names=["Admin"],  # System identity has full administrative access
                email="system@internal"
            )

        # A.2: External API Key or JWT
        if token.startswith("etl_sk_"):
            # API Key resolution (DB bcrypt match)
            user = await crud.resolve_api_key(db, token)
        else:
            # JWT in header (e.g., from SDK that stores token in env)
            username = decode_jwt(token)
            if username:
                user = await crud.get_user_by_username(db, username)

    # --- Path B: HttpOnly Cookie ---
    if user is None and gateway_token:
        username = decode_jwt(gateway_token)
        if username:
            user = await crud.get_user_by_username(db, username)

    if user is None:
        raise api_error(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated. Provide a Bearer token or log in via /api/v1/auth/login.",
            code="not_authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user
