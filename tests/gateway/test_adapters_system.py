import pytest
import json
from datetime import datetime
from unittest.mock import AsyncMock, patch, MagicMock

from gateway.adapters.system import SystemAdapter
from gateway.models.intent import UserIntent
from gateway.models.user import User
from gateway.core.rbac import DEFAULT_ROLES, rbac_provider


@pytest.fixture
def test_user():
    rbac_provider._roles = dict(DEFAULT_ROLES)
    return User(username="admin", hashed_password="x", role_names=["Admin"])


@pytest.mark.asyncio
async def test_system_adapter_health_redis(test_user):
    adapter = SystemAdapter()
    intent = UserIntent(action="health", domain="system", user_id="admin", roles=["Admin"])

    mock_snapshot = {
        "timestamp": datetime.utcnow().isoformat(),
        "services": {
            "ray": {"healthy": True, "error": None},
            "kafka": {"healthy": False, "error": "Connection refused"}
        }
    }

    mock_client = AsyncMock()
    mock_client.lindex.return_value = json.dumps(mock_snapshot)
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__.return_value = None

    with patch("gateway.core.redis.get_redis_client", return_value=mock_client):
        result = await adapter.execute(test_user, intent)

    assert result["status"] == "degraded"
    assert result["source"] == "redis (overseer)"
    assert result["components"]["ray"]["healthy"] is True
    assert result["components"]["kafka"]["healthy"] is False
    assert result["components"]["kafka"]["error"] == "Connection refused"
    mock_client.lindex.assert_called_once_with("overseer:snapshots", 0)


@pytest.mark.asyncio
async def test_system_adapter_health_fallback(test_user):
    adapter = SystemAdapter()
    intent = UserIntent(action="health", domain="system", user_id="admin", roles=["Admin"])

    with patch("gateway.core.redis.get_redis_client", side_effect=Exception("Redis down")):
        result = await adapter.execute(test_user, intent)

    assert result["status"] == "error"
    assert "Redis down" in result["message"]
