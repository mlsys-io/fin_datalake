"""
Shared Context Store for agent collaboration.
Enables agents to share state and coordinate on tasks.
"""
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import ray


@dataclass
class ContextEntry:
    """Wrapper for context entries with TTL support."""
    value: Any
    created_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = None
    owner: Optional[str] = None


@ray.remote
class ContextStore:
    """
    Shared memory store for agent collaboration.
    
    Enables agents to share context, state, and intermediate results.
    Supports TTL for automatic cleanup.
    
    Usage:
        context = ContextStore.remote()
        
        # Store context
        ray.get(context.set.remote("research_result", data, ttl=3600))
        
        # Retrieve context
        result = ray.get(context.get.remote("research_result"))
    """
    
    def __init__(self):
        from etl.utils.logging import setup_logging
        from loguru import logger

        setup_logging(component="context")

        self._store: Dict[str, ContextEntry] = {}
        self._write_count: int = 0
        logger.info("ContextStore initialized")
    
    def set(self, key: str, value: Any, ttl: int = None, owner: str = None) -> bool:
        """
        Store a value in the context.
        
        Args:
            key: Context key
            value: Value to store
            ttl: Time-to-live in seconds (None = no expiry)
            owner: Name of the agent that set this value
            
        Returns:
            True if stored successfully
        """
        expires_at = None
        if ttl:
            expires_at = datetime.utcnow() + timedelta(seconds=ttl)
        
        self._store[key] = ContextEntry(
            value=value,
            expires_at=expires_at,
            owner=owner
        )
        
        # Periodic cleanup: every 100 writes, purge expired entries
        self._write_count += 1
        if self._write_count % 100 == 0:
            self._cleanup_expired()
        
        from loguru import logger
        logger.debug(f"Context set: '{key}' (ttl={ttl}s, owner={owner})")
        return True
    
    def append(self, key: str, item: Any, ttl: int = None, owner: str = None) -> bool:
        """
        Append an item to a list in the context (Atomic operation).
        
        Args:
            key: Context key
            item: Item to append
            ttl: Update TTL if provided
            owner: Update owner if provided
        """
        entry = self._store.get(key)
        
        if entry is None:
            # Create new list
            value = [item]
        else:
            if not isinstance(entry.value, list):
                # Convert to list if existing value is single item (optional behavior)
                # or raise error. For now, we assume usage is consistent.
                from loguru import logger
                logger.warning(f"Context key '{key}' is not a list, overwriting with list.")
                value = [item]
            else:
                entry.value.append(item)
                value = entry.value
        
        return self.set(key, value, ttl, owner)
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Retrieve a value from the context.
        
        Args:
            key: Context key
            default: Default value if not found or expired
            
        Returns:
            The stored value or default
        """
        entry = self._store.get(key)
        
        if entry is None:
            return default
        
        # Check expiry
        if entry.expires_at and datetime.utcnow() > entry.expires_at:
            del self._store[key]
            return default
        
        return entry.value
    
    def delete(self, key: str) -> bool:
        """Remove a key from the context."""
        if key in self._store:
            del self._store[key]
            return True
        return False
    
    def exists(self, key: str) -> bool:
        """Check if a key exists and is not expired."""
        return self.get(key) is not None
    
    def keys(self, pattern: str = None) -> list:
        """
        List all keys, optionally filtered by prefix pattern.
        
        Args:
            pattern: Optional prefix to filter keys
        """
        self._cleanup_expired()
        
        if pattern:
            return [k for k in self._store.keys() if k.startswith(pattern)]
        return list(self._store.keys())
    
    def get_info(self, key: str) -> Optional[Dict]:
        """Get metadata about a context entry."""
        entry = self._store.get(key)
        if entry is None:
            return None
        
        return {
            "created_at": entry.created_at.isoformat(),
            "expires_at": entry.expires_at.isoformat() if entry.expires_at else None,
            "owner": entry.owner
        }
    
    def _cleanup_expired(self):
        """Remove all expired entries."""
        now = datetime.utcnow()
        expired = [
            k for k, v in self._store.items()
            if v.expires_at and now > v.expires_at
        ]
        for k in expired:
            del self._store[k]


def get_context() -> ray.actor.ActorHandle:
    """
    Get or create the singleton ContextStore actor.
    
    Thread-safe: handles race condition where two processes
    try to create the store simultaneously.
    
    Returns:
        Ray ActorHandle to the ContextStore
    """
    try:
        return ray.get_actor("ContextStore")
    except ValueError:
        try:
            return ContextStore.options(
                name="ContextStore", lifetime="detached"
            ).remote()
        except ValueError:
            # Another process created it between our check and create
            return ray.get_actor("ContextStore")
