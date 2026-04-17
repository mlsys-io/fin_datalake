"""
Tests for the Gateway identity models.
"""

from gateway.models.user import APIKey, User


class TestUser:
    def test_default_role_is_analyst(self):
        user = User(username="test", hashed_password="x")
        assert user.role_names == ["Analyst"]
        assert user.is_active is True
        assert user.email is None

    def test_custom_roles_are_preserved(self):
        user = User(username="admin", hashed_password="x", role_names=["Admin", "Analyst"])
        assert user.username == "admin"
        assert user.hashed_password == "x"
        assert user.role_names == ["Admin", "Analyst"]


class TestAPIKey:
    def test_dataclass_fields(self):
        api_key = APIKey(
            key_prefix="etl_sk_abc123",
            key_hash="hashed",
            user_id="admin",
        )
        assert api_key.key_prefix == "etl_sk_abc123"
        assert api_key.key_hash == "hashed"
        assert api_key.user_id == "admin"
        assert api_key.description == "Default API Key"
        assert api_key.is_active is True
