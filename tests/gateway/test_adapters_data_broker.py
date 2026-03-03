"""
Tests for the DataAdapter and BrokerAdapter.

Focuses on action routing, output format, and permission enforcement.
External backends (DuckDB, Prefect, Ray) are NOT called — only the
adapter's own dispatch logic is exercised.
"""

import pytest
from unittest.mock import patch
from gateway.adapters.data import DataAdapter
from gateway.adapters.broker import BrokerAdapter
from gateway.core.adapters import ActionNotFoundError, PermissionError
from gateway.models.intent import UserIntent
from gateway.models.user import User


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_intent(domain: str, action: str, **params) -> UserIntent:
    return UserIntent(
        domain=domain, action=action, parameters=params,
        user_id="test", role="admin",
    )

ADMIN = User(username="admin", hashed_password="x", role_names=["Admin"])
ANALYST = User(username="analyst", hashed_password="x", role_names=["Analyst"])


# ---------------------------------------------------------------------------
# DataAdapter
# ---------------------------------------------------------------------------

class TestDataAdapter:
    def setup_method(self):
        self.adapter = DataAdapter()

    def test_handles_returns_data(self):
        assert self.adapter.handles() == "data"

    def test_list_tables_returns_dict_with_tables_key(self):
        intent = make_intent("data", "list_tables")
        result = self.adapter.execute(ANALYST, intent)
        assert "tables" in result
        assert isinstance(result["tables"], list)

    def test_list_tables_entries_have_name_and_path(self):
        intent = make_intent("data", "list_tables")
        result = self.adapter.execute(ANALYST, intent)
        for table in result["tables"]:
            assert "name" in table
            assert "path" in table

    def test_unknown_action_raises(self):
        intent = make_intent("data", "nonexistent")
        with pytest.raises(ActionNotFoundError):
            self.adapter.execute(ADMIN, intent)


# ---------------------------------------------------------------------------
# BrokerAdapter
# ---------------------------------------------------------------------------

class TestBrokerAdapter:
    def setup_method(self):
        self.adapter = BrokerAdapter()

    def test_handles_returns_broker(self):
        assert self.adapter.handles() == "broker"

    def test_list_connections_allowed_for_admin(self):
        intent = make_intent("broker", "list_connections")
        result = self.adapter.execute(ADMIN, intent)
        assert "available_connections" in result

    def test_list_connections_has_minio_and_timescale(self):
        intent = make_intent("broker", "list_connections")
        result = self.adapter.execute(ADMIN, intent)
        names = [c["name"] for c in result["available_connections"]]
        assert "minio" in names
        assert "timescaledb" in names

    @patch.dict("os.environ", {"AWS_ACCESS_KEY_ID": "test-key", "AWS_SECRET_ACCESS_KEY": "test-secret"})
    def test_get_s3_creds_returns_credentials(self):
        intent = make_intent("broker", "get_s3_creds")
        result = self.adapter.execute(ADMIN, intent)
        assert result["access_key_id"] == "test-key"
        assert result["secret_access_key"] == "test-secret"

    def test_get_s3_creds_denied_for_analyst(self):
        """Analyst has broker:read but NOT broker:vend."""
        intent = make_intent("broker", "get_s3_creds")
        with pytest.raises(PermissionError):
            self.adapter.execute(ANALYST, intent)

    def test_unknown_action_raises(self):
        intent = make_intent("broker", "fake_action")
        with pytest.raises(ActionNotFoundError):
            self.adapter.execute(ADMIN, intent)
