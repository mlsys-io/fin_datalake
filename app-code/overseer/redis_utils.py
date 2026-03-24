"""
Central Redis connection factory for the Overseer.
"""
from typing import Optional
from redis.asyncio import Redis
from etl.config import config

def get_redis_client() -> Optional[Redis]:
    """Returns a Redis client using the central configuration, or None if REDIS_URL is unset."""
    if not config.REDIS_URL:
        return None
    return Redis.from_url(config.REDIS_URL, decode_responses=True)
