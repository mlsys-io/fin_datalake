"""
System Router — /api/v1/system
Dedicated control endpoints for system-level operations (backpressure, maintenance, etc).
"""

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel

from gateway.api.deps import get_current_user
from gateway.models.user import User
from gateway.core.rbac import Permission, rbac_provider
from gateway.api.errors import api_error
from gateway.services.system import fetch_overseer_alerts, fetch_overseer_snapshots, probe_infra_targets

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
        raise api_error(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Forbidden: SYSTEM_ADMIN permission required.",
            code="permission_denied",
        )

    state = body.state.lower()
    ttl = body.ttl
    reason = body.reason

    from gateway.core.redis import get_redis_client
    r = get_redis_client()
    if not r:
        raise api_error(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Redis not configured",
            code="redis_not_configured",
        )
    
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
    try:
        return await fetch_overseer_snapshots(n)
    except Exception as e:
        raise api_error(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
            code="overseer_snapshots_unavailable",
        )

@router.get("/overseer/alerts", summary="Retrieve recent autonomic actions taken by the Overseer")
async def get_overseer_alerts(
    n: int = 20,
    user: User = Depends(get_current_user),
) -> list:
    """
    Fetch recent alert logs from Overseer Redis (db 1).
    Uses a dedicated connection to avoid contaminating the shared gateway Redis pool.
    """
    try:
        return await fetch_overseer_alerts(n)
    except Exception as e:
        raise api_error(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
            code="overseer_alerts_unavailable",
        )

@router.get("/infra/status", summary="Probe internal UI targets such as Prefect and Ray")
async def get_infra_status(
    user: User = Depends(get_current_user),
) -> dict:
    """
    Perform short health probes against internal dashboard targets so the
    frontend can avoid loading hanging iframes when a service is down.
    """
    return await probe_infra_targets()
