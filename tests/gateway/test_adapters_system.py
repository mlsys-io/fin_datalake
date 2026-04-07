import pytest
import json
from datetime import datetime
from unittest.mock import patch, MagicMock

from gateway.adapters.system import SystemAdapter
from gateway.models.intent import UserIntent
from gateway.models.user import User, Role, Permission


@pytest.fixture
def test_user():
    return User(
        username="admin",
        roles=[
            Role(
                name="admin",
                permissions=[Permission.SYSTEM_READ]
            )
        ]
    )


def test_system_adapter_health_redis(test_user):
    adapter = SystemAdapter()
    intent = UserIntent(action="health", domain="system")

    mock_snapshot = {
        "timestamp": datetime.utcnow().isoformat(),
        "services": {
            "ray": {"healthy": True, "error": None},
            "kafka": {"healthy": False, "error": "Connection refused"}
        }
    }

    # Patch the redis.from_url to return our mock client
    with patch("gateway.adapters.system.redis.from_url") as mock_from_url:
        mock_client = MagicMock()
        mock_client.lindex.return_value = json.dumps(mock_snapshot)
        mock_from_url.return_value = mock_client

        result = adapter.execute(test_user, intent)

        # Assertions
        assert result["status"] == "degraded"
        assert result["source"] == "redis (overseer)"
        assert result["components"]["ray"]["healthy"] is True
        assert result["components"]["kafka"]["healthy"] is False
        assert result["components"]["kafka"]["error"] == "Connection refused"
        
        # Verify the client was called properly
        mock_client.lindex.assert_called_once_with("overseer:snapshots", 0)
        mock_client.close.assert_called_once()


def test_system_adapter_health_fallback(test_user):
    adapter = SystemAdapter()
    intent = UserIntent(action="health", domain="system")

    # If Redis raises an exception, it should fallback to the DB query
    with patch("gateway.adapters.system.redis.from_url", side_effect=Exception("Redis down")), \
         patch.object(adapter, "_execute_query") as mock_execute:
        
        # Mock database returning 0 errors for component "gateway" and 2 for "overseer"
        mock_execute.return_value = [
            ("gateway", 0, 0, 100),
            ("overseer", 2, 0, 50)
        ]

        result = adapter.execute(test_user, intent)

        assert "source" not in result  # Not from Redis
        assert result["status"] == "degraded"
        assert result["components"]["gateway"]["healthy"] is True
        assert result["components"]["overseer"]["healthy"] is False
        assert result["components"]["overseer"]["errors"] == 2
        
        mock_execute.assert_called_once()
