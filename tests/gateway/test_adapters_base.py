"""
Tests for BaseAdapter permission enforcement and ActionNotFoundError.
"""

import pytest
from gateway.core.adapters import BaseAdapter, ActionNotFoundError
from gateway.models.user import Permission, User
from gateway.models.intent import UserIntent


class ConcreteAdapter(BaseAdapter):
    """Minimal concrete adapter for testing the base class."""

    def handles(self) -> str:
        return "test"

    def execute(self, user, intent):
        return {"ok": True}


class TestBaseAdapterPermissions:
    def setup_method(self):
        self.adapter = ConcreteAdapter()
        self.admin = User(username="admin", hashed_password="x", role_names=["Admin"])
        self.analyst = User(username="analyst", hashed_password="x", role_names=["Analyst"])

    def test_require_permission_passes_for_admin(self):
        """Admin has SYSTEM_ADMIN, so this should not raise."""
        self.adapter._require_permission(self.admin, Permission.SYSTEM_ADMIN)

    def test_require_permission_raises_for_analyst(self):
        """Analyst does NOT have SYSTEM_ADMIN."""
        with pytest.raises(PermissionError):
            self.adapter._require_permission(self.analyst, Permission.SYSTEM_ADMIN)

    def test_analyst_can_read_data(self):
        """Analyst has DATA_READ, so this should pass."""
        self.adapter._require_permission(self.analyst, Permission.DATA_READ)

    def test_handles_returns_domain(self):
        assert self.adapter.handles() == "test"


class TestActionNotFoundError:
    def test_is_an_exception(self):
        with pytest.raises(ActionNotFoundError):
            raise ActionNotFoundError("no such action")

    def test_message_preserved(self):
        err = ActionNotFoundError("missing action 'foo'")
        assert "foo" in str(err)
