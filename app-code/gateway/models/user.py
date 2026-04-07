"""
Identity Models — Pure data containers for authenticated entities.

Authorization logic is decoupled and handled by the core.rbac module.
"""

from dataclasses import dataclass, field
from typing import List, Optional
from pydantic import BaseModel, Field


@dataclass
class User:
    """
    Represents an authenticated user in the Gateway system.
    Strictly an Identity container.
    """
    username: str
    hashed_password: str
    role_names: List[str] = field(default_factory=lambda: ["Analyst"])
    is_active: bool = True
    email: Optional[str] = None


@dataclass
class APIKey:
    """
    A programmatic API key tied to a User.
    """
    key_prefix: str        # First 16 chars (e.g. "etl_sk_abc123")
    key_hash: str          # bcrypt hash of the full key
    user_id: str           # Owner's username
    description: str = "Default API Key"
    is_active: bool = True
