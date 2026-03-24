from typing import Any, Dict, Union
from abc import abstractmethod
from .base import BaseAgent

class LangChainAgent(BaseAgent):
    """
    A specialized Agent for LangChain workloads.
    Enforces that the executor follows the `Runnable` protocol.
    """

    @abstractmethod
    def build_executor(self):
        """
        Must return a LangChain Runnable (Chain, AgentExecutor, etc).
        """
        pass
    
    def ask(self, payload: Union[str, Dict[str, Any]], session_id: Optional[str] = None) -> Any:
        """
        Handle session-based context by passing it to the base class.
        """
        return super().ask(payload, session_id=session_id)
