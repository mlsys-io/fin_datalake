"""
Tests for the GatewayActuator (Overseer -> Gateway communication).
"""

import pytest
import httpx
from unittest.mock import AsyncMock, patch
from overseer.actuators.gateway import GatewayActuator
from overseer.models import OverseerAction, ActionType, ActionResult


@pytest.mark.asyncio
async def test_gateway_actuator_success():
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
        assert "OPENED" in result.detail
        
        # Verify the payload sent to Gateway
        args, kwargs = mock_post.call_args
        json_data = kwargs["json"]
        assert "api/v1/system/circuit-breaker" in args[0]
        assert json_data["state"] == "open"
        assert json_data["ttl"] == 120


@pytest.mark.asyncio
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
