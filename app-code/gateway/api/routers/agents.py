"""
Agents Router - /api/v1/agents

Dedicated REST endpoints for AgentHub and related operational UI flows.
These routes remain thin wrappers over the shared dispatch pipeline.
"""

from typing import Any, Dict

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel

from gateway.api.deps import get_current_user, get_registry
from gateway.core.adapters import ActionNotFoundError, PermissionError
from gateway.core.dispatch import CircuitBreakerOpenError, dispatch
from gateway.core.registry import DomainNotFoundError, InterfaceRegistry
from gateway.models.user import User
from gateway.api.errors import api_error

router = APIRouter()


class AgentChatRequest(BaseModel):
    message: str
    session_id: str | None = None


class AgentInvokeRequest(BaseModel):
    payload: Any
    session_id: str | None = None


class AgentBroadcastRequest(BaseModel):
    payload: Dict[str, Any]


async def _dispatch_agent_action(
    *,
    user: User,
    registry: InterfaceRegistry,
    action: str,
    parameters: Dict[str, Any] | None = None,
) -> Any:
    try:
        result = await dispatch(
            registry=registry,
            user=user,
            domain="agent",
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


@router.get("", summary="List all registered agents")
async def list_agents(
    user: User = Depends(get_current_user),
    registry: InterfaceRegistry = Depends(get_registry),
) -> Any:
    return await _dispatch_agent_action(
        user=user,
        registry=registry,
        action="list",
    )


@router.post("/{agent_name}/chat", summary="Chat with a named agent")
async def chat_with_agent(
    agent_name: str,
    body: AgentChatRequest,
    user: User = Depends(get_current_user),
    registry: InterfaceRegistry = Depends(get_registry),
) -> Any:
    return await _dispatch_agent_action(
        user=user,
        registry=registry,
        action="chat",
        parameters={
            "agent_name": agent_name,
            "message": body.message,
            "session_id": body.session_id,
        },
    )


@router.post("/{agent_name}/invoke", summary="Invoke a named agent with an arbitrary payload")
async def invoke_agent(
    agent_name: str,
    body: AgentInvokeRequest,
    user: User = Depends(get_current_user),
    registry: InterfaceRegistry = Depends(get_registry),
) -> Any:
    return await _dispatch_agent_action(
        user=user,
        registry=registry,
        action="invoke",
        parameters={
            "agent_name": agent_name,
            "payload": body.payload,
            "session_id": body.session_id,
        },
    )


@router.post("/broadcast", summary="Broadcast an event to all alive agents")
async def broadcast_agent_event(
    body: AgentBroadcastRequest,
    user: User = Depends(get_current_user),
    registry: InterfaceRegistry = Depends(get_registry),
) -> Any:
    return await _dispatch_agent_action(
        user=user,
        registry=registry,
        action="notify",
        parameters={"payload": body.payload},
    )
