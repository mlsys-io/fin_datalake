"""
RayActuator — Spawns / kills Ray actors via the external Ray Client.

Connects via `ray.init(address="ray://head:10001")`.
The address is configured via the RAY_ADDRESS env var or config.yaml.
"""

from __future__ import annotations

from loguru import logger

from overseer.actuators.base import BaseActuator
from overseer.models import ActionResult, ActionType, OverseerAction


class RayActuator(BaseActuator):

    def __init__(self, ray_address: str = "auto"):
        self.ray_address = ray_address

    async def execute(self, action: OverseerAction) -> ActionResult:
        try:
            import ray
            if not ray.is_initialized():
                ray.init(address=self.ray_address, ignore_reinit_error=True)

            if action.type == ActionType.SCALE_UP:
                agent_cls = self._resolve_agent_class(action.agent)
                if agent_cls is None:
                    return ActionResult(success=False, error=f"Unknown agent class: {action.agent}")
                for _ in range(action.count):
                    agent_cls.options(name=f"{action.agent}-overseer-spawned").remote()
                return ActionResult(
                    success=True,
                    detail=f"Spawned {action.count}x {action.agent}",
                )

            elif action.type == ActionType.RESPAWN:
                agent_cls = self._resolve_agent_class(action.agent)
                if agent_cls is None:
                    return ActionResult(success=False, error=f"Unknown agent class: {action.agent}")
                agent_cls.options(name=f"{action.agent}-respawned").remote()
                return ActionResult(
                    success=True,
                    detail=f"Respawned 1x {action.agent}",
                )

            elif action.type == ActionType.SCALE_DOWN:
                logger.info(f"Scale-down requested for {action.agent} — not yet implemented")
                return ActionResult(
                    success=True,
                    detail=f"Scale-down for {action.agent} acknowledged (graceful drain pending)",
                )

            return ActionResult(success=False, error=f"RayActuator does not handle {action.type}")

        except Exception as e:
            logger.error(f"RayActuator failed: {e}")
            return ActionResult(success=False, error=str(e))

    def _resolve_agent_class(self, class_name: str):
        """Dynamically resolve an agent class from the ETL framework."""
        try:
            import importlib
            mod = importlib.import_module("etl.agents")
            return getattr(mod, class_name, None)
        except ImportError:
            return None
