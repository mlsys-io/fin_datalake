"""
Script to register new dynamic data targets with the Overseer via Redis.
"""
from gateway.core.redis import get_redis_client
import asyncio
from loguru import logger

async def seed_targets():
    redis = get_redis_client()
    # Registering a Delta Lake table dynamic target
    target_key = "overseer:targets:delta_lake"
    table_name = "demo_news"
    table_uri = "s3://delta-lake/demo/bronze/news"
    
    # HSET key field value
    await redis.hset(target_key, table_name, table_uri)
    logger.info(f"Registered data target: {table_uri} under {target_key}")

if __name__ == "__main__":
    asyncio.run(seed_targets())
