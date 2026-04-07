"""
Unified Dispatch Pipeline for the Gateway.

Consolidates cross-cutting concerns (audit logging, circuit breaker,
tracing) into a single async chokepoint used by all interfaces.
"""

import uuid
import time
import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from gateway.core.adapters import ActionNotFoundError, PermissionError
from gateway.core.registry import InterfaceRegistry, DomainNotFoundError
from gateway.models.intent import UserIntent
from gateway.models.user import User
from gateway.core.redis import get_redis_client
from gateway.db.session import AsyncSessionLocal
from gateway.db.audit_log import AuditLogORM

logger = logging.getLogger(__name__)

@dataclass
class DispatchResult:
    """Normalized result container for an intent execution."""
    data: Any
    request_id: str
    status_code: int

class CircuitBreakerOpenError(Exception):
    """Raised when the system is in maintenance mode/backpressure is active."""
    pass

async def dispatch(
    registry: InterfaceRegistry,
    user: User,
    domain: str,
    action: str,
    parameters: Dict[str, Any],
    source_protocol: str = "unknown",
) -> DispatchResult:
    """
    The universal intent execution pipeline.

    1. Generates a request_id for tracing.
    2. Checks the circuit breaker (except for 'system' domain).
    3. Builds the UserIntent.
    4. Calls registry.route().
    5. Writes the audit log entry.
    6. Returns a normalized DispatchResult.
    
    Raises:
        CircuitBreakerOpenError, PermissionError, DomainNotFoundError,
        ActionNotFoundError, ValueError, Exception
    """
    request_id = str(uuid.uuid4())
    start_time = time.time()

    # 1. Circuit Breaker Check
    if domain != "system":
        r = get_redis_client()
        if r:
            try:
                # Intent.py uses 'async with r:' for redis.asyncio.Redis
                async with r:
                    is_open = await r.get("gateway:circuit_breaker")
                if is_open == "open":
                    await _log_audit_event_minimal(
                        request_id=request_id,
                        user_id=user.username,
                        domain=domain,
                        action=action,
                        parameters=parameters,
                        source_protocol=source_protocol,
                        status_code=503,
                        duration_ms=0,
                        error_detail="Circuit breaker open"
                    )
                    raise CircuitBreakerOpenError("System is in maintenance mode — backpressure active.")
            except CircuitBreakerOpenError:
                raise
            except Exception as e:
                # Fail-open if Redis is down
                logger.warning(f"Circuit breaker check failed (Redis down): {e}")

    # 2. Build Intent
    intent = UserIntent(
        domain=domain,
        action=action,
        parameters=parameters,
        user_id=user.username,
        roles=user.role_names if user.role_names else ["analyst"],
        request_id=request_id,
    )

    # 3. Route to Registry
    try:
        result = await registry.route(user, intent)
        duration_ms = (time.time() - start_time) * 1000
        await _log_audit_event(intent, source_protocol, 200, duration_ms)
        return DispatchResult(
            data=result,
            request_id=request_id,
            status_code=200
        )
    except PermissionError as e:
        await _log_audit_event(intent, source_protocol, 403, (time.time() - start_time) * 1000, str(e))
        raise
    except DomainNotFoundError as e:
        await _log_audit_event(intent, source_protocol, 404, (time.time() - start_time) * 1000, str(e))
        raise
    except ActionNotFoundError as e:
        await _log_audit_event(intent, source_protocol, 422, (time.time() - start_time) * 1000, str(e))
        raise
    except ValueError as e:
        await _log_audit_event(intent, source_protocol, 400, (time.time() - start_time) * 1000, str(e))
        raise
    except Exception as e:
        await _log_audit_event(intent, source_protocol, 500, (time.time() - start_time) * 1000, str(e))
        raise

async def _log_audit_event(intent: UserIntent, source_protocol: str, status_code: int, duration_ms: float, error_detail: str = None):
    """Writes the audit log using the UserIntent object."""
    await _log_audit_event_minimal(
        request_id=intent.request_id,
        user_id=intent.user_id,
        domain=intent.domain,
        action=intent.action,
        parameters=intent.parameters,
        source_protocol=source_protocol,
        status_code=status_code,
        duration_ms=duration_ms,
        error_detail=error_detail
    )

async def _log_audit_event_minimal(
    request_id: str,
    user_id: str,
    domain: str,
    action: str,
    parameters: Dict[str, Any],
    source_protocol: str,
    status_code: int,
    duration_ms: float,
    error_detail: str = None
):
    """Internal helper to write the audit log row."""
    try:
        async with AsyncSessionLocal() as db:
            audit_log = AuditLogORM(
                request_id=request_id,
                user_id=user_id,
                domain=domain,
                action=action,
                parameters=json.dumps(parameters) if parameters else "{}",
                source_protocol=source_protocol,
                status_code=status_code,
                duration_ms=duration_ms,
                error_detail=error_detail,
            )
            db.add(audit_log)
            await db.commit()
    except Exception as e:
        logger.error(f"Failed to write audit log: {e}")
