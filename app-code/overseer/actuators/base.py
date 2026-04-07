"""
Base Actuator — Abstract interface for executing OverseerActions.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from overseer.models import ActionResult, OverseerAction


class BaseActuator(ABC):
    @abstractmethod
    async def execute(self, action: OverseerAction) -> ActionResult:
        ...
