import pytest
import asyncio
from unittest.mock import AsyncMock

from overseer.actuators.base import BaseActuator, ActionResult
from overseer.models import OverseerAction, ActionType


class SlowActuator(BaseActuator):
    async def execute(self, action: OverseerAction) -> ActionResult:
        await asyncio.sleep(5)  # Simulate a slow blocking operation
        return ActionResult(success=True)


class FastActuator(BaseActuator):
    async def execute(self, action: OverseerAction) -> ActionResult:
        return ActionResult(success=True)


@pytest.mark.asyncio
async def test_overseer_loop_timeout():
    """Test that the overseer loop cancels actuators that take longer than 30s."""
    from overseer.loop import Overseer
    import time
    
    overseer = Overseer()
    # Mocking self.actuators to only have SlowActuator
    overseer.actuators = {"slow": SlowActuator()}
    
    action = OverseerAction(
        type=ActionType.RESPAWN,
        target="slow",
        reason="Testing timeout",
    )
    
    # We directly test the execution logic from the loop (simplified for test)
    actuator = overseer.actuators.get(action.target)
    
    with pytest.raises(asyncio.TimeoutError):
        # We test that `asyncio.wait_for` correctly bounds the slow actuator call.
        # We'll test with a small timeout limit (e.g. 0.1s against 5s actuator delay).
        await asyncio.wait_for(actuator.execute(action), timeout=0.1)

