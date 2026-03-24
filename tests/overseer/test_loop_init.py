"""
Tests for Overseer class initialization and dynamic policy loading.
"""

import pytest
from unittest.mock import patch, MagicMock
from overseer.loop import Overseer


@pytest.fixture
def mock_overseer_deps():
    with patch("overseer.loop.load_config") as mock_load_config, \
         patch("overseer.loop.load_endpoints") as mock_load_endpoints, \
         patch("overseer.loop.MetricsStore"), \
         patch("overseer.loop.CooldownTracker"):
        
        mock_load_endpoints.return_value = []
        yield mock_load_config


def test_overseer_init_starts_with_zero_policies(mock_overseer_deps):
    """Since all policies come from S3, init should produce an empty list."""
    mock_load_config = mock_overseer_deps
    mock_load_config.return_value = {"overseer": {}}
    
    overseer = Overseer()
    
    assert len(overseer.policies) == 0


def test_overseer_init_has_expected_actuators(mock_overseer_deps):
    """Verify the actuator registry is populated with the known set."""
    mock_load_config = mock_overseer_deps
    mock_load_config.return_value = {"overseer": {}}
    
    overseer = Overseer()
    
    assert "ray" in overseer.actuators
    assert "alert" in overseer.actuators
    assert "gateway" in overseer.actuators
    assert "reporter" in overseer.actuators
    assert "webhook" in overseer.actuators


def test_overseer_init_reads_custom_policy_config(mock_overseer_deps):
    """Verify custom policy S3 config is read from overseer config block."""
    mock_load_config = mock_overseer_deps
    mock_load_config.return_value = {
        "overseer": {
            "custom_policies": {
                "s3_uri": "s3://my-bucket/policies/",
                "sync_interval_seconds": 300,
                "local_dir": "/custom/path/",
            }
        }
    }
    
    overseer = Overseer()
    
    assert overseer.s3_uri == "s3://my-bucket/policies/"
    assert overseer.sync_interval_seconds == 300
    assert overseer.local_custom_dir == "/custom/path/"


def test_overseer_init_loop_interval_default(mock_overseer_deps):
    """Verify default loop interval is 15 seconds when not specified."""
    mock_load_config = mock_overseer_deps
    mock_load_config.return_value = {"overseer": {}}
    
    overseer = Overseer()
    
    assert overseer.loop_interval == 15


def test_overseer_init_loop_interval_custom(mock_overseer_deps):
    """Verify loop interval can be overridden from config."""
    mock_load_config = mock_overseer_deps
    mock_load_config.return_value = {"overseer": {"loop_interval_seconds": 30}}
    
    overseer = Overseer()
    
    assert overseer.loop_interval == 30
