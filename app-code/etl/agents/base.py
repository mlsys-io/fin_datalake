"""
Base Agent class for AI Agents running as Ray Actors.
Heavy imports (ray, loguru) are deferred to runtime methods.
"""
from typing import Any, Dict, List, Optional, TYPE_CHECKING
from abc import abstractmethod

from etl.core.base_service import ServiceTask

if TYPE_CHECKING:
    import ray


class BaseAgent(ServiceTask):
    """
    The 'Container' for an AI Agent.
    
    This class handles the infrastructure (Ray Actor lifecycle, messaging),
    while the subclass defines the intelligence (LLM, Chain, etc.) via `build_executor`.
    
    Deployment:
        Use ``deploy()`` to create a named Ray Actor with all methods
        remotely callable. This is the recommended way to start agents.

        >>> handle = MyAgent.deploy(name="MyAgent")
        >>> ray.get(handle.ask.remote("question"))

    Class Attributes:
        CAPABILITIES: List of capabilities this agent provides (for registry discovery)
    """
    
    CAPABILITIES: list = []  # Override in subclass with agent's capabilities
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(name=self.__class__.__name__, config=config)
        self.executor = None

    # =========================================================================
    # Deployment — extends ServiceTask.deploy() with agent setup
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
    ):
        """
        Deploy this agent as a named Ray Actor.

        Extends ``ServiceTask.deploy()`` by calling ``setup()`` after creation,
        which builds the executor and auto-registers with the AgentRegistry.

        Args:
            name:              Actor name for discovery (defaults to class name).
            num_cpus:          CPU reservation per actor (0 = fractional scheduling).
            max_concurrency:   How many remote calls the actor can handle at once.
            lifetime:          Ray actor lifetime ("detached" survives driver exit).
            config:            Optional config dict passed to __init__.
            **ray_options:     Extra options forwarded to ray.remote().

        Returns:
            A SyncHandle with all methods (ask, notify, delegate, …) callable.
        """
        sync_handle = super().deploy(
            name=name,
            num_cpus=num_cpus,
            max_concurrency=max_concurrency,
            lifetime=lifetime,
            config=config,
            **ray_options,
        )

        # setup() must run AFTER the actor is fully alive and named,
        # so the AgentRegistry can resolve the actor handle via ray.get_actor().
        # SyncHandle translates this into ray.get(handle.setup.remote())
        sync_handle.setup()

        return sync_handle

    # =========================================================================
    # Lifecycle
    # =========================================================================

    def setup(self):
        """
        Called once when the Actor starts.
        Builds the heavy logic (LLM, Tools) to ensure they live on the Cluster Node.
        Also auto-registers with the AgentHub if CAPABILITIES are defined.

        Idempotent — safe to call multiple times; only runs once.
        """
        if self.executor is not None:
            return  # Already set up

        from etl.utils.logging import setup_logging
        from loguru import logger

        setup_logging(component="agent")
        
        logger.info(f"[{self.name}] Setting up Agent Intelligence...")
        self.executor = self.build_executor()
        
        # Auto-register with AgentHub if capabilities defined
        if self.CAPABILITIES:
            self._register_with_hub()
        
        logger.info(f"[{self.name}] Ready.")
    
    def _register_with_hub(self):
        """Register this agent with the AgentHub."""
        import ray
        from loguru import logger
        
        try:
            from etl.agents.hub import get_hub
            hub = get_hub()
            ray.get(hub.register.remote(
                name=self.name,
                capabilities=self.CAPABILITIES,
                metadata={"class": self.__class__.__name__}
            ))
            logger.info(f"[{self.name}] Registered with AgentHub (capabilities: {self.CAPABILITIES})")
        except Exception as e:
            logger.warning(f"[{self.name}] Failed to register with AgentHub: {e}")

    def _deregister_from_hub(self):
        """Remove this agent from the AgentHub on shutdown."""
        import ray
        from loguru import logger

        try:
            from etl.agents.hub import get_hub
            hub = get_hub()
            ray.get(hub.unregister.remote(self.name))
            logger.info(f"[{self.name}] Deregistered from AgentHub")
        except Exception:
            pass  # Best-effort; hub may already be gone

    @abstractmethod
    def build_executor(self) -> Any:
        """
        USER IMPLEMENTATION REQUIRED.
        Must return a callable object (e.g. LangChain AgentExecutor, Function, or Runnable).
        
        The returned object must support `invoke(input)` OR `__call__(input)`.
        """
        pass

    def run(self):
        """
        ServiceTask.run() implementation.

        For agents deployed via ``deploy()``, you typically do NOT call ``run()``
        — the actor stays alive via ``lifetime="detached"`` and handles
        ``ask()`` / ``notify()`` calls on-demand.

        ``run()`` is kept for backward compatibility with the ``as_ray_actor()``
        proxy pattern used by streaming ServiceTasks.
        """
        self.running = True
        self.setup()
        
        import time
        try:
            while self.running:
                time.sleep(1.0)
        finally:
            self.on_stop()

    def shutdown(self):
        """
        Gracefully shut down this agent.

        Calls ``on_stop()`` to run cleanup hooks (deregister, close connections),
        then terminates the Ray Actor process via ``ray.actor.exit_actor()``.

        Usage::

            ray.get(handle.shutdown.remote())  # Agent cleans up and exits
        """
        from loguru import logger

        logger.info(f"[{self.name}] Shutting down...")
        self.running = False
        self.on_stop()

        import ray
        ray.actor.exit_actor()

    def on_stop(self):
        """
        Lifecycle hook: called before the actor shuts down.

        Override in subclass to release resources (close DB connections,
        flush buffers, etc.). Default implementation deregisters from the
        AgentHub.

        Called automatically by ``shutdown()`` and when ``run()`` exits.
        """
        self._deregister_from_hub()

    # =========================================================================
    # Interaction — Request/Response and Pub/Sub
    # =========================================================================

    def ask(self, payload: Any) -> Any:
        """
        Synchronous Request/Response Interaction.
        Called by API Gateway or other Tasks.
        """
        from loguru import logger
        
        if not self.executor:
            # Safety check if called before setup() has been called
            self.setup()
            
        try:
            # Support LangChain 'invoke' protocol
            if hasattr(self.executor, "invoke"):
                return self.executor.invoke(payload)
            # Support standard Callable
            elif callable(self.executor):
                return self.executor(payload)
            else:
                raise TypeError(f"Executor {type(self.executor)} is not callable and has no 'invoke' method.")
        except Exception as e:
            logger.error(f"[{self.name}] Error during ask(): {e}")
            raise

    def notify(self, event: Dict[str, Any]):
        """
        Handle incoming messages from the MessageBus.
        
        Routes messages to topic-specific handlers if they exist:
        - Topic "market_data" → calls self.handle_market_data(payload)
        - Topic "alert" → calls self.handle_alert(payload)
        - Otherwise → calls self.on_message(event)
        
        Subclasses should override handle_<topic>() or on_message() to process.
        """
        from loguru import logger
        
        try:
            topic = event.get("topic", "")
            payload = event.get("payload", {})
            
            # Look for topic-specific handler: handle_<topic>()
            handler_name = f"handle_{topic.replace('-', '_').replace('.', '_')}"
            handler = getattr(self, handler_name, None)
            
            if handler and callable(handler):
                logger.debug(f"[{self.name}] Routing to {handler_name}")
                handler(payload, event)
            else:
                # Default handler
                self.on_message(event)
                
        except Exception as e:
            logger.error(f"[{self.name}] Error processing notification: {e}")
    
    def on_message(self, event: Dict[str, Any]):
        """
        Default message handler. Override in subclass to process unhandled topics.
        
        Args:
            event: Full message dict with topic, payload, sender, timestamp
        """
        from loguru import logger
        logger.info(f"[{self.name}] Received: {event.get('topic')} from {event.get('sender')}")

    # =========================================================================
    # Delegation — Cross-Agent Task Routing
    # =========================================================================

    def delegate(
        self, 
        capability: str, 
        payload: Any,
        retry_on_failure: bool = True,
        max_retries: int = 3
    ) -> Any:
        """
        Delegate a task to another agent that has the required capability.
        
        Routes through the AgentHub, which handles discovery + retry.
        
        Args:
            capability: The capability needed (e.g., "analysis", "data_fetch")
            payload: The input to pass to the target agent
            retry_on_failure: If True, try other capable agents on failure (default: True)
            max_retries: Maximum number of agents to try (default: 3)
            
        Returns:
            Result from the delegated agent
            
        Raises:
            ValueError: If no agent with the capability is found
            RuntimeError: If all capable agents fail
        """
        import ray
        from loguru import logger
        from etl.agents.hub import get_hub
        
        logger.info(f"[{self.name}] Delegating '{capability}'")
        hub = get_hub()
        return ray.get(hub.call_by_capability.remote(
            capability=capability,
            payload=payload,
            retry_on_failure=retry_on_failure,
            max_retries=max_retries,
        ))
