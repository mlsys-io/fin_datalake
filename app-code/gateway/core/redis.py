"""
Central Redis connection factory for the Gateway.
"""
from redis.asyncio import Redis
from gateway.core import config

def get_redis_client() -> Redis:
    """Returns a Redis client using the central configuration."""
    if not config.REDIS_URL:
        return None
    return Redis.from_url(config.REDIS_URL, decode_responses=True)
