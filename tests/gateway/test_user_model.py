"""
Tests for the Gateway IAM model: Permission, Policy, Role, User.

Covers:
  - Permission enum values
  - Role → Policy → Permission resolution
  - User.all_permissions() deduplication
  - User.has_permission() checks
  - BUILTIN_ROLES mapping
"""

import pytest
from gateway.models.user import (
    BUILTIN_ROLES,
    Permission,
    Policy,
    Role,
    User,
)


# ---------------------------------------------------------------------------
# Permission Enum
# ---------------------------------------------------------------------------

class TestPermission:
    def test_data_read_value(self):
        assert Permission.DATA_READ == "data:read"

    def test_system_admin_value(self):
        assert Permission.SYSTEM_ADMIN == "system:admin"

    def test_permission_is_string(self):
        """Permissions are usable as plain strings (str enum)."""
        assert isinstance(Permission.COMPUTE_WRITE, str)


# ---------------------------------------------------------------------------
# Role & Policy
# ---------------------------------------------------------------------------

class TestRole:
    def test_analyst_has_data_read(self):
        analyst = BUILTIN_ROLES["Analyst"]
        assert analyst.has_permission(Permission.DATA_READ)

    def test_analyst_cannot_write_data(self):
        analyst = BUILTIN_ROLES["Analyst"]
        assert not analyst.has_permission(Permission.DATA_WRITE)

    def test_admin_has_system_admin(self):
        admin = BUILTIN_ROLES["Admin"]
        assert admin.has_permission(Permission.SYSTEM_ADMIN)

    def test_admin_has_all_data_permissions(self):
        admin = BUILTIN_ROLES["Admin"]
        assert admin.has_permission(Permission.DATA_READ)
        assert admin.has_permission(Permission.DATA_WRITE)

    def test_data_engineer_has_compute_write(self):
        de = BUILTIN_ROLES["DataEngineer"]
        assert de.has_permission(Permission.COMPUTE_WRITE)

    def test_role_permissions_are_deduplicated(self):
        """A role with overlapping policies should not duplicate permissions."""
        overlap_policy_a = Policy(name="A", permissions=[Permission.DATA_READ, Permission.COMPUTE_READ])
        overlap_policy_b = Policy(name="B", permissions=[Permission.DATA_READ])
        role = Role(name="test", policies=[overlap_policy_a, overlap_policy_b])
        perms = role.permissions
        assert perms.count(Permission.DATA_READ) == 1


# ---------------------------------------------------------------------------
# User
# ---------------------------------------------------------------------------

class TestUser:
    def test_default_role_is_analyst(self):
        user = User(username="test", hashed_password="x")
        assert user.role_names == ["Analyst"]

    def test_all_permissions_deduplication(self):
        """Admin includes Analyst permissions; all_permissions() should deduplicate."""
        user = User(username="admin", hashed_password="x", role_names=["Admin"])
        perms = user.all_permissions()
        unique = set(perms)
        assert len(perms) == len(unique), "all_permissions() returned duplicates"

    def test_has_permission_positive(self):
        user = User(username="admin", hashed_password="x", role_names=["Admin"])
        assert user.has_permission(Permission.SYSTEM_ADMIN)

    def test_has_permission_negative(self):
        user = User(username="analyst", hashed_password="x", role_names=["Analyst"])
        assert not user.has_permission(Permission.SYSTEM_ADMIN)

    def test_unknown_role_is_ignored(self):
        """An unrecognized role name should not crash, just be silently skipped."""
        user = User(username="test", hashed_password="x", role_names=["FakeRole"])
        assert user.roles == []
        assert user.all_permissions() == []

    def test_multi_role_merges_permissions(self):
        user = User(username="power", hashed_password="x", role_names=["Analyst", "DataEngineer"])
        perms = user.all_permissions()
        assert Permission.DATA_READ in perms       # from Analyst
        assert Permission.COMPUTE_WRITE in perms   # from DataEngineer
