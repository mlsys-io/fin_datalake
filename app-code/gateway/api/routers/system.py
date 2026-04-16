"""
System Router — /api/v1/system
Dedicated control endpoints for system-level operations (backpressure, maintenance, etc).
"""

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel

from gateway.api.deps import get_current_user, get_registry
from gateway.core.adapters import ActionNotFoundError, AdapterExecutionError, PermissionError
from gateway.core.dispatch import CircuitBreakerOpenError, dispatch
from gateway.core.registry import DomainNotFoundError, InterfaceRegistry
from gateway.models.user import User
from gateway.core.rbac import Permission, rbac_provider
from gateway.api.errors import api_error

router = APIRouter()

class CircuitBreakerRequest(BaseModel):
    state: str  # "open" or "closed"
    ttl: int = 60
    reason: str = "No reason provided"


async def _dispatch_system_action(
    *,
    user: User,
    registry: InterfaceRegistry,
    action: str,
    parameters: dict | None = None,
) -> dict:
    try:
        result = await dispatch(
            registry=registry,
            user=user,
            domain="system",
            action=action,
            parameters=parameters or {},
            source_protocol="rest",
        )
        return result.data
    except CircuitBreakerOpenError as e:
        raise api_error(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(e),
            code="circuit_breaker_open",
        )
    except PermissionError as e:
        raise api_error(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(e),
            code="permission_denied",
        )
    except AdapterExecutionError as e:
        raise api_error(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=str(e),
            code="adapter_execution_failed",
        )
    except DomainNotFoundError as e:
        raise api_error(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
            code="domain_not_found",
        )
    except ActionNotFoundError as e:
        raise api_error(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(e),
            code="action_not_found",
        )
    except ValueError as e:
        raise api_error(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
            code="invalid_request",
        )

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
    registry: InterfaceRegistry = Depends(get_registry),
) -> list:
    """
    Fetch historical heartbeat snapshots from Overseer Redis (db 1).
    Uses a dedicated connection to avoid contaminating the shared gateway Redis pool.
    """
    payload = await _dispatch_system_action(
        user=user,
        registry=registry,
        action="overseer_snapshots",
        parameters={"limit": n},
    )
    return payload.get("snapshots", payload)

@router.get("/overseer/alerts", summary="Retrieve recent autonomic actions taken by the Overseer")
async def get_overseer_alerts(
    n: int = 20,
    user: User = Depends(get_current_user),
    registry: InterfaceRegistry = Depends(get_registry),
) -> list:
    """
    Fetch recent alert logs from Overseer Redis (db 1).
    Uses a dedicated connection to avoid contaminating the shared gateway Redis pool.
    """
    payload = await _dispatch_system_action(
        user=user,
        registry=registry,
        action="overseer_alerts",
        parameters={"limit": n},
    )
    return payload.get("alerts", payload)

@router.get("/infra/status", summary="Probe internal UI targets such as Prefect and Ray")
async def get_infra_status(
    user: User = Depends(get_current_user),
    registry: InterfaceRegistry = Depends(get_registry),
) -> dict:
    """
    Perform short health probes against internal dashboard targets so the
    frontend can avoid loading hanging iframes when a service is down.
    """
    return await _dispatch_system_action(
        user=user,
        registry=registry,
        action="infra_status",
    )


@router.get("/audit-logs", summary="Query persisted gateway audit logs")
async def get_audit_logs(
    since: str = "1h",
    limit: int = 100,
    request_id: str | None = None,
    source_protocol: str | None = None,
    domain: str | None = None,
    action: str | None = None,
    status_code: int | None = None,
    user_id: str | None = None,
    user: User = Depends(get_current_user),
    registry: InterfaceRegistry = Depends(get_registry),
) -> dict:
    """
    Return persisted audit log entries through the same gateway dispatch path.
    """
    return await _dispatch_system_action(
        user=user,
        registry=registry,
        action="audit_logs",
        parameters={
            "since": since,
            "limit": limit,
            "request_id": request_id,
            "source_protocol": source_protocol,
            "domain": domain,
            "action": action,
            "status_code": status_code,
            "user_id": user_id,
        },
    )


@router.get("/interfaces", summary="Inspect gateway interfaces, routes, and MCP tools")
async def get_interface_inventory(
    user: User = Depends(get_current_user),
    registry: InterfaceRegistry = Depends(get_registry),
) -> dict:
    """
    Return a live inventory of the gateway-adjacent interface surface.
    """
    return await _dispatch_system_action(
        user=user,
        registry=registry,
        action="interface_inventory",
    )
