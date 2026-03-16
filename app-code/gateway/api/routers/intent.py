"""
Intent Router — /api/v1/intent

The Universal Endpoint. The central REST entry point for all Gateway operations.
Receives HTTP requests, builds a UserIntent, and delegates to the InterfaceRegistry.

Design:
  This router intentionally has ONE primary endpoint (POST /intent).
  This keeps the API surface minimal and ensures all requests go through
  the same auth + routing pipeline.

  In addition to the universal endpoint, convenience endpoints are provided
  for common patterns (e.g., GET /intent/domains).

Endpoints:
  POST /intent     — Execute any UserIntent
  GET  /intent/domains — List available registered domains
"""

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from typing import Any, Dict

from gateway.api.deps import get_current_user, get_registry
from gateway.core.adapters import ActionNotFoundError, PermissionError
from gateway.core.registry import InterfaceRegistry, DomainNotFoundError
from gateway.models.intent import UserIntent
from gateway.models.user import User

import os
from redis.asyncio import Redis

router = APIRouter()


# ---------------------------------------------------------------------------
# Request/Response Schemas
# ---------------------------------------------------------------------------

class IntentRequest(BaseModel):
    """
    HTTP request body for POST /intent.

    Maps directly to the fields of a UserIntent.
    user_id and role are injected server-side from the authenticated user.
    """
    domain: str
    action: str
    parameters: Dict[str, Any] = {}

    class Config:
        json_schema_extra = {
            "examples": [
                {
                    "summary": "SQL query against Delta Lake",
                    "value": {
                        "domain": "data",
                        "action": "run_sql",
                        "parameters": {"sql": "SELECT * FROM market_data LIMIT 10"},
                    }
                },
                {
                    "summary": "Trigger an ETL pipeline",
                    "value": {
                        "domain": "compute",
                        "action": "submit_job",
                        "parameters": {"pipeline": "kafka_to_delta"},
                    }
                },
                {
                    "summary": "Chat with a named agent",
                    "value": {
                        "domain": "agent",
                        "action": "chat",
                        "parameters": {"agent_name": "MarketAnalyst", "message": "Buy or sell AAPL?"},
                    }
                },
            ]
        }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/intent", summary="Execute a UserIntent against the Gateway")
async def execute_intent(
    body: IntentRequest,
    user: User = Depends(get_current_user),
    registry: InterfaceRegistry = Depends(get_registry),
) -> Any:
    """
    The universal Gateway entry point.

    1. Parses the HTTP request body into an IntentRequest.
    2. Injects authenticated user identity (from auth dependency).
    3. Builds a fully qualified UserIntent.
    4. Routes it through the InterfaceRegistry.
    5. Returns the adapter's serializable result.

    Error handling maps adapter exceptions to appropriate HTTP status codes.
    """
    intent = UserIntent(
        domain=body.domain,
        action=body.action,
        parameters=body.parameters,
        user_id=user.username,
        role=user.role_names[0] if user.role_names else "analyst",
    )

    # -----------------------------------------------------------------------
    # Circuit Breaker Check
    # -----------------------------------------------------------------------
    if intent.domain != "system":
        from gateway.core.redis import get_redis_client
        r = get_redis_client()
        if r:
            try:
                async with r:
                    is_open = await r.get("gateway:circuit_breaker")
                if is_open == "open":
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail="System is in maintenance mode — backpressure active."
                    )
            except HTTPException:
                raise
            except Exception:
                # If Redis is down, we fail-open for reliability
                pass

    try:
        return await registry.route(user, intent)
    except PermissionError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
    except DomainNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ActionNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@router.get("/intent/domains", summary="List all registered capability domains")
def list_domains(
    user: User = Depends(get_current_user),
    registry: InterfaceRegistry = Depends(get_registry),
):
    """
    Returns the list of currently registered domains (e.g., data, compute, agent, broker).
    Useful for API clients to discover what capabilities are available.
    """
    return {"domains": registry.registered_domains()}
