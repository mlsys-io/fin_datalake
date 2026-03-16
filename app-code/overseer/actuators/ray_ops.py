"""
RayActuator - Spawns and kills Ray actors via the external Ray Client.

Connects via ray.init(address="ray://head:10001").
The address is configured via the RAY_ADDRESS env var or config.yaml.
"""

from __future__ import annotations

from loguru import logger
from uuid import uuid4

from overseer.actuators.base import BaseActuator
from overseer.models import ActionResult, ActionType, OverseerAction
from overseer.agent_registry import resolve_agent_class
from overseer.redis_utils import get_redis_client


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
                    actor_name = self._make_actor_name(action.agent)
                    self._deploy_agent(agent_cls, actor_name)
                    await self._track_actor(action.agent, actor_name)
                return ActionResult(
                    success=True,
                    detail=f"Spawned {action.count}x {action.agent}",
                )

            if action.type == ActionType.RESPAWN:
                agent_cls = self._resolve_agent_class(action.agent)
                if agent_cls is None:
                    return ActionResult(success=False, error=f"Unknown agent class: {action.agent}")
                actor_name = self._make_actor_name(action.agent)
                self._deploy_agent(agent_cls, actor_name)
                await self._track_actor(action.agent, actor_name)
                return ActionResult(
                    success=True,
                    detail=f"Respawned 1x {action.agent}",
                )

            if action.type == ActionType.SCALE_DOWN:
                removed = await self._scale_down(action.agent, action.count)
                return ActionResult(
                    success=True,
                    detail=f"Scaled down {removed}x {action.agent}",
                )

            return ActionResult(success=False, error=f"RayActuator does not handle {action.type}")

        except Exception as e:
            logger.error(f"RayActuator failed: {e}")
            return ActionResult(success=False, error=str(e))

    def _resolve_agent_class(self, class_name: str):
        """Dynamically resolve an agent class from registry or etl.agents."""
        resolved = resolve_agent_class(class_name)
        if resolved is not None:
            return resolved
        try:
            import importlib
            mod = importlib.import_module("etl.agents")
            return getattr(mod, class_name, None)
        except ImportError:
            return None

    def _make_actor_name(self, class_name: str) -> str:
        """Generate a unique, traceable actor name."""
        return f"{class_name}-overseer-{uuid4().hex[:8]}"

    def _deploy_agent(self, agent_cls, actor_name: str) -> None:
        """Deploy an agent via its deploy() helper when available."""
        if hasattr(agent_cls, "deploy") and callable(getattr(agent_cls, "deploy")):
            agent_cls.deploy(name=actor_name)
        else:
            agent_cls.options(name=actor_name, lifetime="detached").remote()

    async def _track_actor(self, class_name: str, actor_name: str) -> None:
        """Record spawned actors for future scale-down."""
        r = get_redis_client()
        if not r:
            return
        key = f"overseer:actors:{class_name}"
        async with r:
            await r.lpush(key, actor_name)
            await r.ltrim(key, 0, 999)

    async def _scale_down(self, class_name: str, count: int) -> int:
        """Scale down by killing tracked actors."""
        r = get_redis_client()
        if not r:
            logger.warning("Redis not configured - cannot scale down safely.")
            return 0

        key = f"overseer:actors:{class_name}"
        async with r.pipeline(transaction=True) as pipe:
            for _ in range(count):
                pipe.rpop(key)
            results = await pipe.execute()

        actor_names = [name for name in results if name]
        removed = 0
        import ray
        for name in actor_names:
            try:
                handle = ray.get_actor(name)
                ray.kill(handle, no_restart=True)
                removed += 1
            except ValueError:
                continue
        return removed
