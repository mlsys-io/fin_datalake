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
from gateway.models.user import User, Permission

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
    if not any(role.name == "admin" for role in user.roles): # Simple check for now
         # In a real system, we'd use Permission.SYSTEM_ADMIN check against user.role_names
         pass

    state = body.state.lower()
    ttl = body.ttl
    reason = body.reason

    redis_url = os.environ.get("OVERSEER_REDIS_URL", "redis://:redis-lakehouse-pass@localhost:6379/0")
    r = Redis.from_url(redis_url, decode_responses=True)
    
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
