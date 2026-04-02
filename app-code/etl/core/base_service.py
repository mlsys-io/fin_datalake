from typing import Any, Dict, Optional, TYPE_CHECKING, Union
from abc import ABC, abstractmethod

import ray

from etl.core.utils.dependency_aware_mixin import DependencyAwareMixin

if TYPE_CHECKING:
    import ray as ray_typing


class SyncHandle:
    """
    Synchronous wrapper around a Ray ActorHandle.

    Translates ``proxy.method(args)`` into ``ray.get(handle.method.remote(args))``
    for the common synchronous actor call pattern.
    """

    def __init__(self, handle: "ray_typing.actor.ActorHandle"):
        object.__setattr__(self, "_handle", handle)

    @property
    def handle(self) -> "ray_typing.actor.ActorHandle":
        return self._handle

    def __getattr__(self, name: str):
        if name.startswith("async_"):
            actual_name = name[6:]
            remote_method = getattr(self._handle, actual_name)

            def async_call(*args, **kwargs):
                return remote_method.remote(*args, **kwargs)

            return async_call

        remote_method = getattr(self._handle, name)

        def sync_call(*args, **kwargs):
            return ray.get(remote_method.remote(*args, **kwargs))

        return sync_call

    def __repr__(self):
        return f"SyncHandle({self._handle})"


class ServiceTask(ABC, DependencyAwareMixin):
    """
    Base class for persistent Ray actor services.

    ServiceTask is intentionally separate from BaseTask:
    - BaseTask models finite units of work for orchestration.
    - ServiceTask models long-lived actors with a single main loop.
    """

    def __init__(self, name: Optional[str] = None, config: Optional[Dict[str, Any]] = None, **_: Any):
        self.name = name or self.__class__.__name__
        self.config = config or {}
        self.running = False

    @abstractmethod
    def run(self, *args, **kwargs) -> Any:
        """
        Main service loop.

        Implementations should treat this as the single persistent loop of the
        actor and use _begin_run_loop() / _end_run_loop() for lifecycle safety.
        """
        pass

    def _begin_run_loop(self) -> bool:
        """
        Enter the main loop exactly once.

        Returns False when the service is already running.
        """
        from loguru import logger

        if self.running:
            logger.warning(f"[{self.name}] run() ignored because the service loop is already active.")
            return False

        self.running = True
        return True

    def _end_run_loop(self):
        self.running = False

    @classmethod
    def deploy(
        cls,
        name: Optional[str] = None,
        num_cpus: float = 0,
        max_concurrency: int = 1,
        lifetime: str = "detached",
        config: Optional[Dict[str, Any]] = None,
        return_handle: bool = False,
        **ray_options,
    ) -> Union[SyncHandle, "ray_typing.actor.ActorHandle"]:
        """
        Deploy this service as a named Ray Actor.

        max_concurrency defaults to 1 because services are expected to maintain
        mutable actor state around a single main loop.
        By default this returns a SyncHandle for SDK ergonomics. Set
        return_handle=True to receive the raw Ray ActorHandle instead.
        """
        from loguru import logger

        actor_name = name or cls.__name__

        RemoteCls = ray.remote(
            num_cpus=num_cpus,
            max_concurrency=max_concurrency,
            **ray_options,
        )(cls)

        handle = RemoteCls.options(
            name=actor_name,
            lifetime=lifetime,
        ).remote(name=actor_name, config=config)

        logger.info(f"Deployed service '{actor_name}' (class={cls.__name__})")
        if return_handle:
            return handle
        return SyncHandle(handle)

    @classmethod
    def connect(
        cls,
        name: str,
        return_handle: bool = False,
    ) -> Union[SyncHandle, "ray_typing.actor.ActorHandle"]:
        handle = ray.get_actor(name)
        if return_handle:
            return handle
        return SyncHandle(handle)

    def stop(self):
        """
        Default cooperative stop implementation.
        """
        self.running = False

    def get_status(self) -> Dict[str, Any]:
        return {"running": self.running}
