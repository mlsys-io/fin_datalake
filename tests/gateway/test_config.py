"""
Tests for Gateway core config.
"""

from gateway.core.config import JWT_SECRET_KEY, JWT_ALGORITHM, JWT_EXPIRE_MINUTES, REDIS_URL


def test_secret_key_can_be_missing_in_dev():
    assert JWT_SECRET_KEY is None or isinstance(JWT_SECRET_KEY, str)


def test_algorithm_is_hs256():
    assert JWT_ALGORITHM == "HS256"


def test_expire_minutes_is_positive():
    assert isinstance(JWT_EXPIRE_MINUTES, int)
    assert JWT_EXPIRE_MINUTES > 0


def test_redis_url_is_optional():
    assert REDIS_URL is None or isinstance(REDIS_URL, str)
