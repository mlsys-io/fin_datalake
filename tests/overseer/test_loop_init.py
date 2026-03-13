"""
Tests for Overseer class initialization and policy loading.
"""

import pytest
from unittest.mock import patch, MagicMock
from overseer.loop import Overseer
from overseer.policies.scaling import KafkaLagPolicy
from overseer.policies.healing import ActorHealthPolicy


@pytest.fixture
def mock_overseer_deps():
    with patch("overseer.loop.load_config") as mock_load_config, \
         patch("overseer.loop.load_endpoints") as mock_load_endpoints, \
         patch("overseer.loop.MetricsStore"), \
         patch("overseer.loop.CooldownTracker"):
        
        mock_load_endpoints.return_value = []
        yield mock_load_config


def test_overseer_init_default_policies(mock_overseer_deps):
    mock_load_config = mock_overseer_deps
    mock_load_config.return_value = {"overseer": {}}
    
    overseer = Overseer()
    
    # Should have both default policies
    policy_classes = [p.__class__ for p in overseer.policies]
    assert KafkaLagPolicy in policy_classes
    assert ActorHealthPolicy in policy_classes
    assert len(overseer.policies) == 2


def test_overseer_init_custom_policies(mock_overseer_deps):
    mock_load_config = mock_overseer_deps
    mock_load_config.return_value = {
        "overseer": {
            "policies": ["actor_health"]
        }
    }
    
    overseer = Overseer()
    
    # Should only have ActorHealthPolicy
    policy_classes = [p.__class__ for p in overseer.policies]
    assert ActorHealthPolicy in policy_classes
    assert KafkaLagPolicy not in policy_classes
    assert len(overseer.policies) == 1


def test_overseer_init_empty_policies(mock_overseer_deps):
    mock_load_config = mock_overseer_deps
    mock_load_config.return_value = {
        "overseer": {
            "policies": []
        }
    }
    
    overseer = Overseer()
    
    assert len(overseer.policies) == 0


def test_overseer_init_invalid_policy_skips(mock_overseer_deps):
    mock_load_config = mock_overseer_deps
    mock_load_config.return_value = {
        "overseer": {
            "policies": ["nonexistent_policy", "kafka_lag"]
        }
    }
    
    overseer = Overseer()
    
    # Should only have KafkaLagPolicy
    policy_classes = [p.__class__ for p in overseer.policies]
    assert KafkaLagPolicy in policy_classes
    assert len(overseer.policies) == 1
