"""
Central Redis connection factory for the Overseer.
"""
from redis.asyncio import Redis
from etl.config import config

def get_redis_client() -> Redis:
    """Returns a Redis client using the central configuration."""
    if not config.REDIS_URL:
        return None
    return Redis.from_url(config.REDIS_URL, decode_responses=True)
