"""
Shared Context Store for agent collaboration.
Backed by Redis for horizontal scalability and high throughput.
"""
from typing import Dict, Any, Optional
import json
from datetime import datetime, timedelta
import redis
import os

class ContextStore:
    """
    Redis-backed wrapper for agent collaboration.
    
    Enables agents to share context, state, and intermediate results.
    Supports TTL natively via Redis.
    """
    
    def __init__(self, redis_url: str = None):
        from etl.utils.logging import setup_logging
        from loguru import logger
        
        setup_logging(component="context")
        
        url = redis_url or os.getenv("OVERSEER_REDIS_URL", "redis://localhost:6379/0")
        self._client = redis.Redis.from_url(url, decode_responses=True)
        logger.info(f"ContextStore initialized with Redis: {url}")
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None, owner: Optional[str] = None) -> bool:
        """Store a value in the context (Atomic)."""
        payload = json.dumps(value)
        
        if ttl is not None:
            ttl = int(ttl)
            
        pipeline = self._client.pipeline()
        pipeline.set(key, payload, ex=ttl)
        
        meta = {
            "created_at": datetime.utcnow().isoformat(),
            "owner": owner
        }
        if ttl:
            meta["expires_at"] = (datetime.utcnow() + timedelta(seconds=ttl)).isoformat()
        pipeline.set(f"{key}:meta", json.dumps(meta), ex=ttl)
            
        pipeline.execute()
        return True
    
    def append(self, key: str, item: Any, ttl: Optional[int] = None, owner: Optional[str] = None) -> bool:
        """Append an item to a list in the context."""
        # Read-Modify-Write (for exact semantic match with old implementation)
        # Using a Redis transaction (WATCH) to prevent race conditions
        with self._client.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(key)
                    current = pipe.get(key)
                    if current:
                        value = json.loads(current)
                        if not isinstance(value, list):
                            value = [item]
                        else:
                            value.append(item)
                    else:
                        value = [item]
                    
                    if ttl is not None:
                        ttl = int(ttl)
                    
                    pipe.multi()
                    pipe.set(key, json.dumps(value), ex=ttl)
                    
                    meta = {
                        "created_at": datetime.utcnow().isoformat(),
                        "owner": owner
                    }
                    if ttl:
                        meta["expires_at"] = (datetime.utcnow() + timedelta(seconds=ttl)).isoformat()
                    pipe.set(f"{key}:meta", json.dumps(meta), ex=ttl)
                        
                    pipe.execute()
                    break
                except redis.WatchError:
                    continue # Re-try if the key was modified
        return True
    
    def get(self, key: str, default: Any = None) -> Any:
        """Retrieve a value from the context."""
        val = self._client.get(key)
        if val is None:
            return default
        try:
            return json.loads(val)
        except json.JSONDecodeError:
            return default
    
    def delete(self, key: str) -> bool:
        """Remove a key and its meta from the context."""
        count = self._client.delete(key, f"{key}:meta")
        return count > 0
    
    def exists(self, key: str) -> bool:
        """Check if a key exists."""
        return self._client.exists(key) > 0
    
    def keys(self, pattern: str = None) -> list:
        """List all data keys (ignoring meta keys)."""
        search = pattern or "*"
        all_keys = self._client.keys(search)
        return [k for k in all_keys if not k.endswith(":meta")]
    
    def get_info(self, key: str) -> Optional[Dict]:
        """Get metadata about a context entry."""
        meta = self._client.get(f"{key}:meta")
        if meta:
            return json.loads(meta)
        return None

# Singleton instance wrapper
_context_store_instance = None

def get_context() -> ContextStore:
    """
    Get the singleton ContextStore instance.
    """
    global _context_store_instance
    if _context_store_instance is None:
        _context_store_instance = ContextStore()
    return _context_store_instance
