"""
Tests for the GatewayActuator (Overseer -> Gateway communication).
"""

import pytest
import httpx
from unittest.mock import AsyncMock, patch
from overseer.actuators.gateway import GatewayActuator
from overseer.models import OverseerAction, ActionType, ActionResult


@pytest.mark.asyncio
async def test_gateway_actuator_missing_url():
    """When GATEWAY_INTERNAL_URL is not set, execute should fail gracefully."""
    actuator = GatewayActuator()
    action = OverseerAction(
        type=ActionType.CIRCUIT_BREAK,
        target="gateway",
    )
    
    with patch.dict("os.environ", {}, clear=True):
        result = await actuator.execute(action)
    
    assert result.success is False
    assert "GATEWAY_INTERNAL_URL" in result.error


@pytest.mark.asyncio
@patch.dict("os.environ", {
    "GATEWAY_INTERNAL_URL": "http://gateway:8000",
    "GATEWAY_INTERNAL_TOKEN": "test-secret-token",
})
async def test_gateway_actuator_circuit_break_success():
    actuator = GatewayActuator()
    action = OverseerAction(
        type=ActionType.CIRCUIT_BREAK,
        target="gateway",
        count=120,
        reason="Test high load"
    )
    
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.text = "Success"
        
        result = await actuator.execute(action)
        
        assert result.success is True
        assert "OPEN" in result.detail
        
        # Verify the payload sent to Gateway
        args, kwargs = mock_post.call_args
        json_data = kwargs["json"]
        assert "api/v1/system/circuit-breaker" in args[0]
        assert json_data["state"] == "open"
        assert json_data["ttl"] == 120


@pytest.mark.asyncio
@patch.dict("os.environ", {
    "GATEWAY_INTERNAL_URL": "http://gateway:8000",
    "GATEWAY_INTERNAL_TOKEN": "test-secret-token",
})
async def test_gateway_actuator_failure():
    actuator = GatewayActuator()
    action = OverseerAction(
        type=ActionType.CIRCUIT_BREAK,
        target="gateway"
    )
    
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value.status_code = 500
        mock_post.return_value.text = "Internal Server Error"
        
        result = await actuator.execute(action)
        
        assert result.success is False
        assert "rejected" in result.error


@pytest.mark.asyncio
@patch.dict("os.environ", {
    "GATEWAY_INTERNAL_URL": "http://gateway:8000",
    "GATEWAY_INTERNAL_TOKEN": "test-secret-token",
})
async def test_gateway_actuator_non_circuit_break_sends_closed():
    """When the action type is not circuit_break, state should be 'closed'."""
    actuator = GatewayActuator()
    action = OverseerAction(
        type=ActionType.ALERT,
        target="gateway",
    )
    
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.text = "OK"
        
        result = await actuator.execute(action)
        
        assert result.success is True
        assert "CLOSED" in result.detail
        # Verify no grammar error (no "CLOSEDED")
        assert "CLOSEDED" not in result.detail
