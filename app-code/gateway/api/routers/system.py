"""
System Router — /api/v1/system
Dedicated control endpoints for system-level operations (backpressure, maintenance, etc).
"""

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from typing import Optional
import os
from redis.asyncio import Redis

from gateway.api.deps import get_current_user
from gateway.models.user import User
from gateway.core.rbac import Permission, rbac_provider

router = APIRouter()

class CircuitBreakerRequest(BaseModel):
    state: str  # "open" or "closed"
    ttl: int = 60
    reason: str = "No reason provided"

@router.post("/circuit-breaker", summary="Control the system backpressure circuit breaker")
async def set_circuit_breaker(
    body: CircuitBreakerRequest,
    user: User = Depends(get_current_user),
) -> dict:
    """
    Open or close the system-wide circuit breaker.
    Requires SYSTEM_ADMIN permission.
    """
    # Authorization check
    if not rbac_provider.is_authorized(user.role_names, Permission.SYSTEM_ADMIN):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Forbidden: SYSTEM_ADMIN permission required.",
        )

    state = body.state.lower()
    ttl = body.ttl
    reason = body.reason

    from gateway.core.redis import get_redis_client
    r = get_redis_client()
    if not r:
        raise HTTPException(status_code=500, detail="Redis not configured")
    
    async with r:
        if state == "open":
            await r.setex("gateway:circuit_breaker", ttl, "open")
            message = f"Circuit breaker OPENED for {ttl}s. Reason: {reason}"
        else:
            await r.delete("gateway:circuit_breaker")
            message = "Circuit breaker CLOSED."

    return {
        "status": "success",
        "state": state,
        "message": message
    }

@router.get("/overseer/snapshots", summary="Retrieve the last N system snapshots from the Overseer")
async def get_overseer_snapshots(
    n: int = 50,
    user: User = Depends(get_current_user),
) -> list:
    """
    Fetch historical heartbeat snapshots from Overseer Redis (db 1).
    Uses a dedicated connection to avoid contaminating the shared gateway Redis pool.
    """
    import json
    from redis.asyncio import Redis as AsyncRedis
    from gateway.core import config

    if not config.REDIS_URL:
        raise HTTPException(status_code=500, detail="Redis not configured")

    # Build a dedicated db=1 URL robustly using urllib.parse
    from urllib.parse import urlparse, urlunparse
    parsed = urlparse(config.REDIS_URL)
    # The path component of a redis:// URL is the db number (e.g. "/0" or "/1")
    overseer_redis_url = urlunparse(parsed._replace(path='/1'))

    try:
        r = AsyncRedis.from_url(overseer_redis_url, decode_responses=True)
        async with r:
            items = await r.lrange("overseer:snapshots", 0, n - 1)

        result = []
        for item in reversed(items):
            try:
                snap = json.loads(item)
                entry = {
                    "timestamp": snap.get("timestamp"),
                    "services": snap.get("services", {})
                }
                result.append(entry)
            except Exception:
                continue
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/overseer/alerts", summary="Retrieve recent autonomic actions taken by the Overseer")
async def get_overseer_alerts(
    n: int = 20,
    user: User = Depends(get_current_user),
) -> list:
    """
    Fetch recent alert logs from Overseer Redis (db 1).
    Uses a dedicated connection to avoid contaminating the shared gateway Redis pool.
    """
    import json
    from redis.asyncio import Redis as AsyncRedis
    from gateway.core import config

    if not config.REDIS_URL:
        raise HTTPException(status_code=500, detail="Redis not configured")

    from urllib.parse import urlparse, urlunparse
    parsed = urlparse(config.REDIS_URL)
    overseer_redis_url = urlunparse(parsed._replace(path='/1'))

    try:
        r = AsyncRedis.from_url(overseer_redis_url, decode_responses=True)
        async with r:
            items = await r.lrange("overseer:alerts", 0, n - 1)
        return [json.loads(i) for i in items]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
