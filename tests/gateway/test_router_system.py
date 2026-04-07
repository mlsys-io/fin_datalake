import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch, MagicMock

from gateway.api.main import create_app
from gateway.api.deps import get_current_user
from gateway.models.user import User, Permission, Role, BUILTIN_ROLES

# Mock users
ADMIN_USER = User(
    username="admin", 
    hashed_password="x", 
    role_names=["Admin"]
)
ANALYST_USER = User(
    username="analyst", 
    hashed_password="x", 
    role_names=["Analyst"]
)

@pytest.fixture
def client():
    app = create_app()
    # Mock startup events (registry)
    app.state.registry = MagicMock()
    return TestClient(app)

def test_circuit_breaker_forbidden_for_analyst(client):
    # Override dependency to return analyst user
    client.app.dependency_overrides[get_current_user] = lambda: ANALYST_USER
    
    response = client.post(
        "/api/v1/system/circuit-breaker",
        json={"state": "open", "ttl": 60, "reason": "test"}
    )
    
    assert response.status_code == 403
    assert "SYSTEM_ADMIN permission required" in response.json()["detail"]
    # Clean up
    client.app.dependency_overrides = {}

def test_circuit_breaker_allowed_for_admin(client):
    # Override dependency to return admin user
    client.app.dependency_overrides[get_current_user] = lambda: ADMIN_USER
    
    # Mock Redis client
    mock_redis = MagicMock()
    mock_redis.setex = AsyncMock()
    mock_redis.__aenter__ = AsyncMock(return_value=mock_redis)
    mock_redis.__aexit__ = AsyncMock()
    
    with patch("gateway.core.redis.get_redis_client", return_value=mock_redis):
        response = client.post(
            "/api/v1/system/circuit-breaker",
            json={"state": "open", "ttl": 60, "reason": "test"}
        )
    
    assert response.status_code == 200
    assert response.json()["state"] == "open"
    # Clean up
    client.app.dependency_overrides = {}
