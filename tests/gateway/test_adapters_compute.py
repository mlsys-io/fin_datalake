"""
Tests for the ComputeAdapter.
Focuses on async execution and Prefect integration.
"""

import pytest
from unittest.mock import AsyncMock, patch
from gateway.adapters.compute import ComputeAdapter
from gateway.models.intent import UserIntent
from gateway.models.user import User

def make_intent(domain: str, action: str, **params) -> UserIntent:
    return UserIntent(
        domain=domain, action=action, parameters=params,
        user_id="test", role="admin",
    )

ADMIN = User(username="admin", hashed_password="x", role_names=["Admin"])

class TestComputeAdapter:
    def setup_method(self):
        self.adapter = ComputeAdapter()

    @pytest.mark.asyncio
    @patch("prefect.client.orchestration.get_client")
    async def test_get_status_async(self, mock_get_client):
        """Verify that get_status is now async and uses the Prefect client correctly."""
        # Mock Prefect client
        mock_client = AsyncMock()
        mock_get_client.return_value.__aenter__.return_value = mock_client
        
        # Mock flow run response
        mock_flow_run = AsyncMock()
        mock_flow_run.state.type.value = "COMPLETED"
        mock_client.read_flow_run.return_value = mock_flow_run

        intent = make_intent("compute", "get_status", job_id="test-job-123")
        
        # This will fail until Task 14 (BaseAdapter async) is fully applied to the registry,
        # but the adapter itself should now be awaitable.
        result = await self.adapter.execute(ADMIN, intent)
        
        assert result["status"] == "COMPLETED"
        mock_client.read_flow_run.assert_called_once_with("test-job-123")
