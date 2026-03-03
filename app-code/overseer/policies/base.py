"""
Base Policy — Abstract interface for decision rules.

Policies evaluate a SystemSnapshot and return zero or more
OverseerActions to be executed by the Actuator layer.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from overseer.models import OverseerAction, SystemSnapshot


class BasePolicy(ABC):
    """Evaluates system metrics and emits actions."""

    @abstractmethod
    def evaluate(self, snapshot: SystemSnapshot) -> list[OverseerAction]:
        ...
