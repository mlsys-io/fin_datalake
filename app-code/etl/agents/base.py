"""
Base Agent class for AI Agents running as Ray Actors.
Heavy imports (ray, loguru) are deferred to runtime methods.
"""
from typing import Any, Dict, List, Optional, TYPE_CHECKING
from abc import ABC, abstractmethod

from etl.core.base_service import SyncHandle
from etl.agents.mixins import ConversationManagerMixin


from etl.core.constants import OVERSEER_REDIS_URL

if TYPE_CHECKING:
    import ray
    import ray.serve as serve
    from fastapi import FastAPI


class BaseAgent(ABC, ConversationManagerMixin):

    """
    The 'Container' for an AI Agent, now powered by Ray Serve.
    
    This class handles the infrastructure (Ray Serve lifecycle, HTTP ingress),
    while the subclass defines the intelligence (LLM, Chain, etc.) via `build_executor`.
    
    Deployment:
        Use ``deploy()`` to create a Ray Serve application.
        
        >>> handle = MyAgent.deploy(name="MyAgent")
        >>> await handle.ask.remote("question")

    HTTP API:
        - POST /ask
        - POST /notify
    """
    
    CAPABILITIES: list = []  # Override in subclass with agent's capabilities
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.name = self.__class__.__name__
        self.config = config or {}
        self.executor = None
        self.running = False

        # If this instance is created by Ray Serve's ingress, self.app will be set.
        # We bind the FastAPI endpoints here.
        if hasattr(self, 'app') and isinstance(self.app, FastAPI):
            self._bind_endpoints(self.app)

    # =========================================================================
    # Deployment & Connection (Ray Serve)
    # =========================================================================

    @classmethod
    def deploy(
        cls,
        name: Optional[str] = None,
        num_replicas: int = 1,
        num_cpus: float = 0.5,
        config: Optional[Dict[str, Any]] = None,
        **serve_options,
    ):
        """
        Deploy this agent as a Ray Serve application.
        """
        import ray.serve as serve
        from fastapi import FastAPI
        from loguru import logger

        actor_name = name or cls.__name__
        app = FastAPI(title=f"Agent: {actor_name}")
        
        # Define the Ingress class
        # We use a bit of Ray Serve magic here to wrap the class
        deployment_cls = serve.deployment(
            name=actor_name,
            num_replicas=num_replicas,
            ray_actor_options={"num_cpus": num_cpus},
            **serve_options
        )(serve.ingress(app)(cls))

        handle = serve.run(deployment_cls.bind(config=config), name=actor_name)
        
        logger.info(f"Deployed Agent '{actor_name}' to Ray Serve")
        
        # We call setup() via the handle to ensure it's ready
        # In Ray Serve, handles are async by default
        ray.get(handle.setup.remote())
        
        return handle

    @classmethod
    def connect(cls, name: str):
        """Connect to an existing Ray Serve agent."""
        import ray.serve as serve
        return serve.get_app_handle(name)

    # =========================================================================
    # HTTP Endpoints (FastAPI)
    # =========================================================================

    # Note: These are defined on the class but activated via @serve.ingress(app)
    # inside the deploy() method. For subclasses to have these endpoints,
    # they must be present when the FastAPI app is bound.
    
    # Since we can't easily use @app.post inside an abstract class without 
    # the app instance, we'll rely on Ray Serve's ability to expose methods.
    # To keep it standard, we'll add a helper to bind the app.
    
    def _bind_endpoints(self, app: "FastAPI"):
        @app.post("/ask")
        async def ask_endpoint(request: Dict[str, Any]):
            payload = request.get("payload")
            session_id = request.get("session_id")
            return self.ask(payload, session_id=session_id)


        @app.post("/notify")
        async def notify_endpoint(payload: Dict[str, Any]):
            return self.notify(payload)
            
        @app.get("/health")
        async def health_check():
            return {"status": "ok", "agent": self.name}

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
        Shut down this Ray Serve agent.
        """
        import ray.serve as serve
        serve.delete(self.name)
        self.on_stop()

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

    def ask(self, payload: Any, session_id: Optional[str] = None) -> Any:
        """
        Synchronous Request/Response Interaction.
        
        Args:
            payload: The input for the agent
            session_id: Optional session/thread ID for conversation context
        """
        from loguru import logger
        
        if not self.executor:
            self.setup()
            
        history = []
        if session_id:
            logger.debug(f"[{self.name}] Loading history for session: {session_id}")
            history = self.load_conversation_state(session_id)
            
            # Formulate the message for the LLM/Executor
            # If payload is just a string, we assume it's the next user message
            if isinstance(payload, str):
                history.append({"role": "user", "content": payload})
            elif isinstance(payload, dict) and "content" in payload:
                history.append(payload)
            else:
                # Fallback: stringify whatever is in payload
                history.append({"role": "user", "content": str(payload)})
                
            # Trim to prevent context overflow
            history = self.trim_history(history)
            input_data = history
        else:
            input_data = payload
            
        try:
            # Support LangChain 'invoke' protocol
            if hasattr(self.executor, "invoke"):
                result = self.executor.invoke(input_data)
            # Support standard Callable
            elif callable(self.executor):
                result = self.executor(input_data)
            else:
                raise TypeError(f"Executor {type(self.executor)} is not callable and has no 'invoke' method.")
                
            # If session-based, save the response to history
            if session_id:
                # Extract content from result if it's an object (common in LangChain)
                content = result.content if hasattr(result, "content") else str(result)
                history.append({"role": "assistant", "content": content})
                self.save_conversation_state(session_id, history)
                
            return result
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
        Delegate a task to another agent with the required capability.
        
        Now performs "client-side" routing:
        1. Query AgentHub for agents with the capability.
        2. Pick one (round-robin or random).
        3. Call its Ray Serve handle directly.
        """
        import ray
        import random
        from loguru import logger
        from etl.agents.hub import get_hub
        import ray.serve as serve
        
        logger.info(f"[{self.name}] Delegating '{capability}'")
        hub = get_hub()
        agent_names = ray.get(hub.find_by_capability.remote(capability))
        
        if not agent_names:
            raise ValueError(f"No agent found with capability: {capability}")
            
        # For simplicity in this refactor, we pick a random one
        # Micro-Plan 3.5 could add more sophisticated RR here if needed
        attempts = min(len(agent_names), max_retries) if retry_on_failure else 1
        random.shuffle(agent_names)
        
        last_error = None
        for i in range(attempts):
            target_name = agent_names[i]
            try:
                handle = serve.get_app_handle(target_name)
                # Serve handles are async-by-default, we wrap in ray.get for parity with old delegate()
                return ray.get(handle.ask.remote(payload))
            except Exception as e:
                logger.warning(f"[{self.name}] Delegation to {target_name} failed: {e}")
                last_error = e
                
        raise RuntimeError(f"All agents for capability '{capability}' failed. Last error: {last_error}")

