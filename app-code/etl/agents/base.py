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
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(name=self.__class__.__name__, config=config)
        self.executor = None

    def setup(self):
        """
        Called once when the Actor starts.
        Builds the heavy logic (LLM, Tools) to ensure they live on the Cluster Node.
        """
        logger.info(f"[{self.name}] Setting up Agent Intelligence...")
        self.executor = self.build_executor()
        logger.info(f"[{self.name}] Ready.")

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
        Asynchronous Fire-and-Forget.
        Used for Alerts, Triggers, or Stream Processing.
        """
        # In a real impl, this might push to an internal Queue processed by run()
        # For simplicity in V1, we process immediately but don't return result.
        try:
             # We treat notification payload as input to the same executor, 
             # OR we could have a separate handler. 
             # For now, assume the executor handles it or we log it.
             logger.info(f"[{self.name}] Received Notification: {event}")
             # Optional: self.executor.invoke(event) 
        except Exception as e:
            logger.error(f"[{self.name}] Error processing notification: {e}")
