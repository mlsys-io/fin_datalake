from typing import Any, Dict, Optional
from abc import abstractmethod
import ray
from loguru import logger

from etl.core.base_service import ServiceTask

class BaseAgent(ServiceTask):
    """
    The 'Container' for an AI Agent.
    
    This class handles the infrastructure (Ray Actor lifecycle, messaging),
    while the subclass defines the intelligence (LLM, Chain, etc.) via `build_executor`.
    
    Class Attributes:
        CAPABILITIES: List of capabilities this agent provides (for registry discovery)
    """
    
    CAPABILITIES: list = []  # Override in subclass with agent's capabilities
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(name=self.__class__.__name__, config=config)
        self.executor = None

    def setup(self):
        """
        Called once when the Actor starts.
        Builds the heavy logic (LLM, Tools) to ensure they live on the Cluster Node.
        Also auto-registers with the AgentRegistry if CAPABILITIES are defined.
        """
        logger.info(f"[{self.name}] Setting up Agent Intelligence...")
        self.executor = self.build_executor()
        
        # Auto-register with AgentRegistry if capabilities defined
        if self.CAPABILITIES:
            self._register_with_registry()
        
        logger.info(f"[{self.name}] Ready.")
    
    def _register_with_registry(self):
        """Register this agent with the AgentRegistry."""
        try:
            from etl.agents.registry import get_registry
            registry = get_registry()
            ray.get(registry.register.remote(
                name=self.name,
                capabilities=self.CAPABILITIES,
                metadata={"class": self.__class__.__name__}
            ))
            logger.info(f"[{self.name}] Registered with capabilities: {self.CAPABILITIES}")
        except Exception as e:
            logger.warning(f"[{self.name}] Failed to register with AgentRegistry: {e}")

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
        The implementation of ServiceTask.run().
        For an Agent, this is usually just keeping the Actor alive and handling async events.
        """
        self.running = True
        self.setup()
        
        import time
        while self.running:
            # Main Loop: Could check a queue for 'notify' events in the future.
            # For now, we just sleep to keep the Actor alive so it can receive remote calls.
            time.sleep(1.0) 

    def ask(self, payload: Any) -> Any:
        """
        Synchronous Request/Response Interaction.
        Called by API Gateway or other Tasks.
        """
        if not self.executor:
            # Safety check if called before run/setup complete
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
        logger.info(f"[{self.name}] Received: {event.get('topic')} from {event.get('sender')}")

    def delegate(
        self, 
        capability: str, 
        payload: Any,
        retry_on_failure: bool = True,
        max_retries: int = 3
    ) -> Any:
        """
        Delegate a task to another agent that has the required capability.
        
        Uses the AgentRegistry for service discovery. If retry_on_failure is True,
        will try other agents with the same capability if the first one fails.
        
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
        from etl.agents.registry import get_registry
        
        registry = get_registry()
        agents = ray.get(registry.find_by_capability.remote(capability))
        
        if not agents:
            raise ValueError(f"No agent found with capability: {capability}")
        
        # Limit retries to available agents
        attempts = min(len(agents), max_retries) if retry_on_failure else 1
        last_error = None
        
        for i in range(attempts):
            target_name = agents[i]
            
            try:
                target = ray.get(registry.get_agent.remote(target_name))
                
                if target is None:
                    logger.warning(f"[{self.name}] Agent '{target_name}' not accessible, trying next...")
                    continue
                
                logger.info(f"[{self.name}] Delegating '{capability}' to {target_name}")
                return ray.get(target.ask.remote(payload))
                
            except Exception as e:
                last_error = e
                logger.warning(f"[{self.name}] Agent '{target_name}' failed: {e}")
                
                if not retry_on_failure:
                    raise
                
                if i < attempts - 1:
                    logger.info(f"[{self.name}] Retrying with next agent...")
        
        # All agents failed
        raise RuntimeError(
            f"All {attempts} agents with capability '{capability}' failed. "
            f"Last error: {last_error}"
        )

