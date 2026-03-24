"""
Security Utilities — JWT management and password hashing.

This module centralizes all cryptographic operations for the Gateway,
ensuring consistent algorithms and secret management across the 
FastAPI REST layer and MCP tool connectors.
"""

import os
from datetime import datetime, timezone, timedelta
from jose import JWTError, jwt
from passlib.context import CryptContext

from gateway.core.config import JWT_SECRET_KEY, JWT_ALGORITHM, JWT_EXPIRE_MINUTES

# ---------------------------------------------------------------------------
# Password Hashing
# ---------------------------------------------------------------------------

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(plain: str) -> str:
    """Hash a plain-text password using bcrypt."""
    return pwd_context.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    """Verify a plain-text password against its bcrypt hash."""
    return pwd_context.verify(plain, hashed)


# ---------------------------------------------------------------------------
# JWT Management
# ---------------------------------------------------------------------------

def create_jwt(username: str) -> str:
    """
    Create a signed JWT for the given username.
    The token expires after JWT_EXPIRE_MINUTES minutes.
    """
    if not JWT_SECRET_KEY:
        raise RuntimeError("GATEWAY_JWT_SECRET environment variable is not set.")
        
    expire = datetime.now(timezone.utc) + timedelta(minutes=JWT_EXPIRE_MINUTES)
    payload = {"sub": username, "exp": expire}
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)


def decode_jwt(token: str) -> str | None:
    """
    Decode a JWT and return the `sub` claim (username).
    Returns None if the token is invalid or expired.
    """
    if not JWT_SECRET_KEY:
        return None
        
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        return payload.get("sub")
    except JWTError:
        return None
