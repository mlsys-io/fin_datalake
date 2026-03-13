import pytest
import httpx
from unittest.mock import AsyncMock, patch
from overseer.collectors.base import BaseCollector
from overseer.collectors.health import GenericHealthCollector
from overseer.models import ServiceEndpoint


@pytest.fixture
def mock_endpoint():
    return ServiceEndpoint(
        name="test_service",
        host="localhost",
        port=8080,
        protocol="http",
        health_path="/health"
    )

@pytest.mark.asyncio
async def test_generic_collector_healthy(mock_endpoint):
    collector = GenericHealthCollector(mock_endpoint)
    
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json = AsyncMock(return_value={"status": "ok"})
        
        metrics = await collector.collect()
        
        assert metrics.healthy is True
        assert metrics.service == "test_service"
        assert metrics.error is None

@pytest.mark.asyncio
async def test_generic_collector_unhealthy_status(mock_endpoint):
    collector = GenericHealthCollector(mock_endpoint)
    
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value.status_code = 500  # API error
        
        metrics = await collector.collect()
        
        assert metrics.healthy is False
        assert metrics.service == "test_service"

@pytest.mark.asyncio
async def test_generic_collector_connection_error(mock_endpoint):
    collector = GenericHealthCollector(mock_endpoint)
    
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.side_effect = httpx.ConnectError("Connection refused")
        
        metrics = await collector.collect()
        
        assert metrics.healthy is False
        assert metrics.service == "test_service"
        assert "Connection refused" in metrics.error
