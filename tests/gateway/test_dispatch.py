import pytest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from gateway.core.dispatch import dispatch, DispatchResult, CircuitBreakerOpenError
from gateway.models.user import User
from gateway.core.adapters import ActionNotFoundError, PermissionError
from gateway.core.registry import InterfaceRegistry, DomainNotFoundError

@pytest.mark.asyncio
async def test_dispatch_success():
    # Setup
    registry = MagicMock(spec=InterfaceRegistry)
    registry.route = AsyncMock(return_value={"result": "ok"})
    
    user = User(username="testuser", hashed_password="x", role_names=["Analyst"])
    domain = "data"
    action = "query"
    parameters = {"sql": "select 1"}
    source_protocol = "rest"
    
    # Mock Redis (Circuit Breaker Closed)
    mock_redis = AsyncMock()
    mock_redis.get.return_value = "closed"
    mock_redis.__aenter__.return_value = mock_redis
    
    # Mock Session and DB
    mock_session = AsyncMock()
    mock_session.__aenter__.return_value = mock_session
    
    with patch("gateway.core.dispatch.get_redis_client", return_value=mock_redis), \
         patch("gateway.core.dispatch.AsyncSessionLocal", return_value=mock_session), \
         patch("gateway.core.dispatch.AuditLogORM") as MockAuditLog:
        
        # Execute
        result = await dispatch(registry, user, domain, action, parameters, source_protocol)
        
        # Verify
        assert isinstance(result, DispatchResult)
        assert result.data == {"result": "ok"}
        assert result.status_code == 200
        assert uuid.UUID(result.request_id)
        
        # Verify Registry call
        registry.route.assert_called_once()
        
        # Verify Audit Log call (Status 200)
        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()
        
        # Verify AuditLogORM constructor called with correct status
        args, kwargs = MockAuditLog.call_args
        assert kwargs["status_code"] == 200
        assert kwargs["source_protocol"] == "rest"

@pytest.mark.asyncio
async def test_dispatch_circuit_breaker_open():
    registry = MagicMock(spec=InterfaceRegistry)
    user = User(username="testuser", hashed_password="x", role_names=["Analyst"])
    
    mock_redis = AsyncMock()
    mock_redis.get.return_value = "open"
    mock_redis.__aenter__.return_value = mock_redis
    
    mock_session = AsyncMock()
    mock_session.__aenter__.return_value = mock_session

    with patch("gateway.core.dispatch.get_redis_client", return_value=mock_redis), \
         patch("gateway.core.dispatch.AsyncSessionLocal", return_value=mock_session), \
         patch("gateway.core.dispatch.AuditLogORM") as MockAuditLog:
        
        with pytest.raises(CircuitBreakerOpenError):
            await dispatch(registry, user, "data", "query", {}, "rest")
            
        # Verify status 503 logged for circuit breaker
        mock_session.add.assert_called_once()
        args, kwargs = MockAuditLog.call_args
        assert kwargs["status_code"] == 503
        assert "Circuit breaker" in kwargs["error_detail"]

@pytest.mark.asyncio
async def test_dispatch_adapter_error():
    registry = MagicMock(spec=InterfaceRegistry)
    registry.route = AsyncMock(side_effect=PermissionError("Denied"))
    
    user = User(username="testuser", hashed_password="x", role_names=["Analyst"])
    
    mock_redis = AsyncMock()
    mock_redis.get.return_value = "closed"
    mock_redis.__aenter__.return_value = mock_redis
    
    mock_session = AsyncMock()
    mock_session.__aenter__.return_value = mock_session

    with patch("gateway.core.dispatch.get_redis_client", return_value=mock_redis), \
         patch("gateway.core.dispatch.AsyncSessionLocal", return_value=mock_session), \
         patch("gateway.core.dispatch.AuditLogORM") as MockAuditLog:
        
        with pytest.raises(PermissionError):
            await dispatch(registry, user, "data", "query", {}, "rest")
            
        # Verify status 403 logged
        mock_session.add.assert_called_once()
        args, kwargs = MockAuditLog.call_args
        assert kwargs["status_code"] == 403
        assert kwargs["error_detail"] == "Denied"
