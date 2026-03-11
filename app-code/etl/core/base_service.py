from typing import Any, Dict, Optional, Type, TYPE_CHECKING
from abc import abstractmethod
import ray
from etl.core.base_task import BaseTask

if TYPE_CHECKING:
    import ray as ray_typing


class SyncHandle:
    """
    Synchronous wrapper around a Ray ActorHandle.

    Translates ``proxy.method(args)`` into ``ray.get(handle.method.remote(args))``,
    removing boilerplate for the common synchronous call pattern.

    Usage::

        proxy = SyncHandle(handle)

        # Synchronous (blocks until result):
        result = proxy.ask("question")
        status = proxy.get_status()

        # Fire-and-forget (returns ObjectRef immediately):
        ref = proxy.async_notify(event)     # async_ prefix = no ray.get

        # Raw handle for advanced usage:
        proxy.handle.notify.remote(event)
    """

    def __init__(self, handle: "ray_typing.actor.ActorHandle"):
        # Use object.__setattr__ to avoid triggering our __getattr__
        object.__setattr__(self, "_handle", handle)

    @property
    def handle(self) -> "ray_typing.actor.ActorHandle":
        """Access the raw Ray ActorHandle for advanced async patterns."""
        return self._handle

    def __getattr__(self, name: str):
        """
        Intercept method calls and route them through Ray.

        - ``proxy.ask(...)``         → ``ray.get(handle.ask.remote(...))``   (sync)
        - ``proxy.async_ask(...)``   → ``handle.ask.remote(...)``            (fire-and-forget)
        """
        # Fire-and-forget: async_ prefix returns ObjectRef without blocking
        if name.startswith("async_"):
            actual_name = name[6:]  # Strip "async_" prefix
            remote_method = getattr(self._handle, actual_name)

            def async_call(*args, **kwargs):
                return remote_method.remote(*args, **kwargs)

            return async_call

        # Default: synchronous call with ray.get
        remote_method = getattr(self._handle, name)

        def sync_call(*args, **kwargs):
            return ray.get(remote_method.remote(*args, **kwargs))

        return sync_call

    def __repr__(self):
        return f"SyncHandle({self._handle})"


class ServiceTask(BaseTask):
    """
    A Task designed to run indefinitely as a Service (e.g., Kafka Consumer, AI Agent).
    Intended to be deployed as a Ray Actor.

    Quick start::

        # Deploy (creates actor on cluster)
        svc = MyService.deploy(name="MyService")
        svc.run()                                # sync call
        svc.get_status()                         # sync call

        # Connect from another process
        svc = MyService.connect("MyService")     # returns SyncHandle
        svc.get_status()                         # no ray.get/.remote needed

        # Fire-and-forget
        svc.async_run()                          # returns immediately

        # Raw handle for advanced patterns
        svc.handle.run.remote()                  # non-blocking
    """
    
    @abstractmethod
    def run(self, *args, **kwargs) -> Any:
        """
        The main loop of the service. 
        Should typically be an infinite loop.
        """
        pass

    # =========================================================================
    # Deployment & Discovery
    # =========================================================================

    @classmethod
    def deploy(
        cls,
        name: Optional[str] = None,
        num_cpus: float = 0,
        max_concurrency: int = 10,
        lifetime: str = "detached",
        config: Dict[str, Any] = None,
        **ray_options,
    ) -> SyncHandle:
        """
        Deploy this service as a named Ray Actor with full remote method access.

        Applies ``@ray.remote`` directly to the class, so ALL public methods
        are callable remotely via the returned ``SyncHandle``.

        Returns a ``SyncHandle`` for ergonomic usage::

            svc = MyService.deploy(name="MySvc")
            svc.get_status()                   # sync, no boilerplate
            svc.async_run()                    # fire-and-forget
            svc.handle.run.remote()            # raw async when needed

        Args:
            name:              Actor name for discovery (defaults to class name).
            num_cpus:          CPU reservation per actor (0 = fractional scheduling).
            max_concurrency:   How many remote calls the actor can handle at once.
            lifetime:          Ray actor lifetime ("detached" survives driver exit).
            config:            Optional config dict passed to __init__.
            **ray_options:     Extra options forwarded to ray.remote().

        Returns:
            A SyncHandle wrapping the Ray ActorHandle.
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
        ).remote(config=config)

        logger.info(f"Deployed '{actor_name}' (class={cls.__name__})")
        return SyncHandle(handle)

    @classmethod
    def connect(cls, name: str) -> SyncHandle:
        """
        Connect to an existing deployed actor by name.

        Returns a ``SyncHandle`` for ergonomic synchronous calls.
        Raises ``ValueError`` if the actor is not found.

        Usage::

            # From any process connected to the same Ray cluster
            svc = MyService.connect("MyService")
            result = svc.get_status()

        Args:
            name: The actor name used during deploy().

        Returns:
            A SyncHandle wrapping the Ray ActorHandle.
        """
        handle = ray.get_actor(name)
        return SyncHandle(handle)

    def get_status(self) -> Dict[str, Any]:
        """
        Default status implementation. Can be overridden.
        """
        return {"running": getattr(self, "running", False)}



