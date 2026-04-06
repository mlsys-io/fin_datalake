"""
RayActuator - Spawns and kills Ray actors via the external Ray Client.

Connects via ray.init(address="ray://head:10001").
The address is configured via the RAY_ADDRESS env var or config.yaml.
"""

from __future__ import annotations

import asyncio
import os
from loguru import logger
from uuid import uuid4

from overseer.actuators.base import BaseActuator
from overseer.models import ActionResult, ActionType, OverseerAction
from overseer.agent_registry import resolve_agent_class
from overseer.redis_utils import get_redis_client


class RayActuator(BaseActuator):

    def __init__(self, ray_address: str | None = None, ray_namespace: str | None = None):
        self.ray_address = ray_address or os.getenv("RAY_ADDRESS") or "auto"
        self.ray_namespace = ray_namespace or os.getenv("RAY_NAMESPACE") or "serve"
        self.redis = get_redis_client()

    async def execute(self, action: OverseerAction) -> ActionResult:
        try:
            if action.type == ActionType.SCALE_UP:
                return await self._execute_scale_up(action)

            if action.type == ActionType.RESPAWN:
                return await self._execute_respawn(action)

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

    async def _execute_scale_up(self, action: OverseerAction) -> ActionResult:
        actor_names = await asyncio.to_thread(self._scale_up_sync, action)
        for actor_name in actor_names:
            await self._track_actor(action.agent, actor_name)
        return ActionResult(
            success=True,
            detail=f"Spawned {len(actor_names)}x {action.agent}",
        )

    async def _execute_respawn(self, action: OverseerAction) -> ActionResult:
        class_name, deployment_name = await asyncio.to_thread(self._respawn_sync, action)
        await self._track_actor(class_name, deployment_name)
        return ActionResult(
            success=True,
            detail=f"Respawned deployment '{deployment_name}' as {class_name}",
        )

    def _scale_up_sync(self, action: OverseerAction) -> list[str]:
        from etl.runtime import ensure_ray

        ensure_ray(address=self.ray_address, namespace=self.ray_namespace)
        agent_cls = self._resolve_agent_class(action.agent)
        if agent_cls is None:
            raise RuntimeError(f"Unknown agent class: {action.agent}")

        actor_names: list[str] = []
        for _ in range(action.count):
            actor_name = self._make_actor_name(action.agent)
            self._deploy_agent(agent_cls, actor_name)
            actor_names.append(actor_name)
        return actor_names

    def _respawn_sync(self, action: OverseerAction) -> tuple[str, str]:
        from etl.runtime import ensure_ray

        ensure_ray(address=self.ray_address, namespace=self.ray_namespace)
        class_name = action.agent_class or action.agent
        agent_cls = self._resolve_agent_class(class_name)
        if agent_cls is None:
            raise RuntimeError(f"Unknown agent class: {class_name}")

        deployment_name = action.deployment_name or self._make_actor_name(class_name)
        deployment_metadata = dict(action.deployment_metadata or {})
        route_prefix = action.route_prefix or f"/{deployment_name}"
        config = deployment_metadata.get("config")
        num_replicas = int(deployment_metadata.get("num_replicas", 1) or 1)
        num_cpus = float(deployment_metadata.get("num_cpus", 0.5) or 0.5)
        serve_options = dict(deployment_metadata.get("serve_options") or {})
        serve_options.setdefault("route_prefix", route_prefix)

        self._deploy_agent(
            agent_cls,
            deployment_name,
            config=config,
            num_replicas=num_replicas,
            num_cpus=num_cpus,
            serve_options=serve_options,
        )
        return class_name, deployment_name

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

    def _deploy_agent(
        self,
        agent_cls,
        actor_name: str,
        *,
        config: dict | None = None,
        num_replicas: int = 1,
        num_cpus: float = 0.5,
        serve_options: dict | None = None,
    ) -> None:
        """Deploy an agent via its deploy() helper when available."""
        if hasattr(agent_cls, "deploy") and callable(getattr(agent_cls, "deploy")):
            agent_cls.deploy(
                name=actor_name,
                num_replicas=num_replicas,
                num_cpus=num_cpus,
                config=config,
                **(serve_options or {}),
            )
        else:
            agent_cls.options(name=actor_name, lifetime="detached").remote()

    async def _track_actor(self, class_name: str, actor_name: str) -> None:
        """Record spawned actors for future scale-down."""
        if not self.redis:
            return
        key = f"overseer:actors:{class_name}"
        await self.redis.lpush(key, actor_name)
        await self.redis.ltrim(key, 0, 999)

    async def _scale_down(self, class_name: str, count: int) -> int:
        """Scale down by killing tracked actors."""
        if not self.redis:
            logger.warning("Redis not configured - cannot scale down safely.")
            return 0

        key = f"overseer:actors:{class_name}"
        async with self.redis.pipeline(transaction=True) as pipe:
            for _ in range(count):
                pipe.rpop(key)
            results = await pipe.execute()

        actor_names = [name for name in results if name]
        removed = 0
        import ray
        for name in actor_names:
            try:
                handle = ray.get_actor(name, namespace=self.ray_namespace)
                ray.kill(handle, no_restart=True)
                removed += 1
            except ValueError:
                continue
        return removed
