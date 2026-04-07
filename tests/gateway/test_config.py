"""
Tests for Gateway core config: password hashing and JWT handling.
"""

import pytest
from unittest.mock import patch
from gateway.core.config import (
    hash_password,
    verify_password,
    JWT_SECRET,
    JWT_ALGORITHM,
    JWT_EXPIRE_MINUTES,
)


class TestPasswordHashing:
    def test_hash_returns_string(self):
        h = hash_password("test123")
        assert isinstance(h, str)
        assert h != "test123"

    def test_verify_correct_password(self):
        h = hash_password("mypassword")
        assert verify_password("mypassword", h) is True

    def test_verify_wrong_password(self):
        h = hash_password("correct")
        assert verify_password("wrong", h) is False

    def test_different_inputs_produce_different_hashes(self):
        h1 = hash_password("password1")
        h2 = hash_password("password2")
        assert h1 != h2


class TestJWTConfig:
    def test_secret_has_default(self):
        """JWT_SECRET should have a fallback for development."""
        assert JWT_SECRET is not None
        assert len(JWT_SECRET) > 0

    def test_algorithm_is_hs256(self):
        assert JWT_ALGORITHM == "HS256"

    def test_expire_minutes_is_positive(self):
        assert JWT_EXPIRE_MINUTES > 0
