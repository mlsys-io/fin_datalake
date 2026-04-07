"""
Intent Router — /api/v1/intent

The Universal Endpoint. The central REST entry point for all Gateway operations.
Receives HTTP requests and delegates to the unified dispatch pipeline.

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

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel
from typing import Any, Dict

from gateway.api.deps import get_current_user, get_registry
from gateway.core.adapters import ActionNotFoundError, PermissionError
from gateway.core.registry import InterfaceRegistry, DomainNotFoundError
from gateway.core.dispatch import dispatch, CircuitBreakerOpenError
from gateway.models.user import User
from gateway.api.errors import api_error

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
    3. Delegates execution to the unified dispatch pipeline.
    4. Returns the result data.

    Error handling maps dispatch exceptions to appropriate HTTP status codes.
    """
    try:
        result = await dispatch(
            registry=registry,
            user=user,
            domain=body.domain,
            action=body.action,
            parameters=body.parameters,
            source_protocol="rest"
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
    except Exception:
        # Re-raise internal errors as 500 (standard FastAPI behavior)
        raise


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
