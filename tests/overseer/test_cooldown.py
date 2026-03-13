"""
Tests for the CooldownTracker.
"""

import time
import pytest
from overseer.policies.cooldown import CooldownTracker


def test_cooldown_tracker_logic():
    tracker = CooldownTracker(cooldown_seconds=1)
    
    # Initial state
    assert tracker.can_fire("respawn", "ray") is True
    
    # Record an action
    tracker.record("respawn", "ray")
    
    # Should be on cooldown immediately
    assert tracker.can_fire("respawn", "ray") is False
    
    # Different action/target should not be affected
    assert tracker.can_fire("scale_up", "ray") is True
    assert tracker.can_fire("respawn", "kafka") is True
    
    # Wait for cooldown to pass
    time.sleep(1.1)
    assert tracker.can_fire("respawn", "ray") is True


def test_cooldown_tracker_record_updates():
    tracker = CooldownTracker(cooldown_seconds=10)
    tracker.record("respawn", "ray")
    
    # Wait a bit
    time.sleep(0.1)
    
    last_time = tracker._last_fired["respawn:ray"]
    tracker.record("respawn", "ray")
    
    # New time should be greater
    assert tracker._last_fired["respawn:ray"] > last_time
