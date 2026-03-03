"""
Tests for the InterfaceRegistry: adapter registration and intent routing.
"""

import pytest
from gateway.core.registry import InterfaceRegistry, DomainNotFoundError
from gateway.core.adapters import BaseAdapter, ActionNotFoundError, PermissionError
from gateway.models.intent import UserIntent
from gateway.models.user import Permission, User


# ---------------------------------------------------------------------------
# Stub adapters
# ---------------------------------------------------------------------------

class StubDataAdapter(BaseAdapter):
    def handles(self) -> str:
        return "data"

    def execute(self, user, intent):
        if intent.action == "list_tables":
            return {"tables": ["market_data"]}
        raise ActionNotFoundError(f"Unknown action '{intent.action}'")


class StubComputeAdapter(BaseAdapter):
    def handles(self) -> str:
        return "compute"

    def execute(self, user, intent):
        self._require_permission(user, Permission.COMPUTE_WRITE)
        return {"status": "submitted"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_intent(domain: str, action: str, **params) -> UserIntent:
    return UserIntent(
        domain=domain,
        action=action,
        parameters=params,
        user_id="test-user",
        role="admin",
    )


ADMIN = User(username="admin", hashed_password="x", role_names=["Admin"])
ANALYST = User(username="analyst", hashed_password="x", role_names=["Analyst"])


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestRegistration:
    def test_register_and_retrieve(self):
        reg = InterfaceRegistry()
        reg.register(StubDataAdapter())
        assert "data" in reg.registered_domains()
        assert reg.get_adapter("data") is not None

    def test_chaining(self):
        reg = InterfaceRegistry()
        result = reg.register(StubDataAdapter()).register(StubComputeAdapter())
        assert result is reg  # returns self

    def test_registered_domains(self):
        reg = InterfaceRegistry()
        reg.register(StubDataAdapter())
        reg.register(StubComputeAdapter())
        assert sorted(reg.registered_domains()) == ["compute", "data"]

    def test_get_adapter_returns_none_for_unknown(self):
        reg = InterfaceRegistry()
        assert reg.get_adapter("nonexistent") is None


class TestRouting:
    def setup_method(self):
        self.reg = InterfaceRegistry()
        self.reg.register(StubDataAdapter())
        self.reg.register(StubComputeAdapter())

    def test_route_to_data_adapter(self):
        intent = make_intent("data", "list_tables")
        result = self.reg.route(ADMIN, intent)
        assert result == {"tables": ["market_data"]}

    def test_route_unknown_domain_raises(self):
        intent = make_intent("unknown_domain", "foo")
        with pytest.raises(DomainNotFoundError):
            self.reg.route(ADMIN, intent)

    def test_route_unknown_action_raises(self):
        intent = make_intent("data", "nonexistent_action")
        with pytest.raises(ActionNotFoundError):
            self.reg.route(ADMIN, intent)

    def test_route_permission_denied(self):
        """Analyst should NOT have compute:write."""
        intent = make_intent("compute", "submit_job")
        with pytest.raises(PermissionError):
            self.reg.route(ANALYST, intent)

    def test_route_permission_allowed(self):
        """Admin has compute:write."""
        intent = make_intent("compute", "submit_job")
        result = self.reg.route(ADMIN, intent)
        assert result["status"] == "submitted"
