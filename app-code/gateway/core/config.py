"""
Auth Configuration — JWT settings and password hashing.

Reads from environment variables with safe defaults for development.
For production, set these via K8s Secrets / .env file.
"""

import os
from passlib.context import CryptContext

# ---------------------------------------------------------------------------
# JWT Settings
# ---------------------------------------------------------------------------

# openssl rand -hex 32
JWT_SECRET_KEY: str = os.environ.get("GATEWAY_JWT_SECRET")
if not JWT_SECRET_KEY:
    # We don't raise here to allow the module to load, 
    # but the app should likely fail on startup or first auth use.
    pass
JWT_ALGORITHM: str = "HS256"
JWT_EXPIRE_MINUTES: int = int(os.environ.get("GATEWAY_JWT_EXPIRE_MINUTES", "480"))  # 8 hours

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
