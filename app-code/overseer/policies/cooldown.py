"""
Cooldown Tracker — Prevents rapid re-firing of the same action.
"""

import time
from typing import Dict


class CooldownTracker:
    """
    Tracks when specific actions were last fired to enforce a wait period.
    """

    def __init__(self, cooldown_seconds: int = 120):
        self.cooldown_seconds = cooldown_seconds
        self._last_fired: Dict[str, float] = {}  # key (action:target) -> timestamp

    def can_fire(self, action_type: str, target: str) -> bool:
        """
        Returns True if the cooldown period has passed for this specific action/target pair.
        """
        key = f"{action_type}:{target}"
        last_time = self._last_fired.get(key, 0)
        return (time.time() - last_time) >= self.cooldown_seconds

    def record(self, action_type: str, target: str) -> None:
        """
        Records that an action was fired right now.
        """
        key = f"{action_type}:{target}"
        self._last_fired[key] = time.time()
