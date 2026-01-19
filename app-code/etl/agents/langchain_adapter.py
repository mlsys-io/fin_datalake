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
    
    def ask(self, payload: Union[str, Dict[str, Any]]) -> Any:
        """
        Overridden to handle common string-to-dict conversion if needed.
        """
        input_data = payload
        
        # If user passes a raw string but the Chain expects a dict with a "input" key
        # We could auto-wrap, but for V1 we stick to "Low Abstraction" per user request.
        # We pass it through raw. 
        
        return super().ask(input_data)
