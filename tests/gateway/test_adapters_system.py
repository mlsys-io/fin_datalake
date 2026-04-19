import pytest
from datetime import datetime
from unittest.mock import patch

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

    with patch("gateway.services.system.fetch_overseer_snapshots", return_value=[mock_snapshot]) as fetch_snapshots:
        result = await adapter.execute(test_user, intent)

    assert result["status"] == "degraded"
    assert result["source"] == "redis (overseer)"
    assert result["components"]["ray"]["healthy"] is True
    assert result["components"]["kafka"]["healthy"] is False
    assert result["components"]["kafka"]["error"] == "Connection refused"
    fetch_snapshots.assert_called_once_with(1)


@pytest.mark.asyncio
async def test_system_adapter_health_fallback(test_user):
    adapter = SystemAdapter()
    intent = UserIntent(action="health", domain="system", user_id="admin", roles=["Admin"])

    with patch("gateway.services.system.fetch_overseer_snapshots", side_effect=Exception("Redis down")):
        result = await adapter.execute(test_user, intent)

    assert result["status"] == "error"
    assert "Redis down" in result["message"]


@pytest.mark.asyncio
async def test_system_adapter_health_no_snapshots(test_user):
    adapter = SystemAdapter()
    intent = UserIntent(action="health", domain="system", user_id="admin", roles=["Admin"])

    with patch("gateway.services.system.fetch_overseer_snapshots", return_value=[]):
        result = await adapter.execute(test_user, intent)

    assert result == {"status": "unknown", "message": "No health snapshots available"}
