from typing import Any, Dict, Union, Optional
import os
from loguru import logger
from .base import BaseAgent

class LangChainMixin:
    """
    A mixin that provides LangChain utilities to any class.
    
    This avoids forcing a specific execution flow or inheritance 
    on the agent, allowing for cleaner Ray orchestration.
    """

    def _get_llm(self, model_name: str = "gpt-3.5-turbo", temperature: float = 0.0, json_mode: bool = False) -> Any:
        """
        Dynamically initializes a LangChain ChatModel.
        """
        try:
            from langchain_openai import ChatOpenAI
            api_key = os.environ.get("OPENAI_API_KEY")
            
            if api_key and api_key.lower() not in ["dummy", "test", "demo"]:
                kwargs = {
                    "api_key": api_key,
                    "model": model_name,
                    "temperature": temperature
                }
                if json_mode:
                    kwargs["model_kwargs"] = {"response_format": {"type": "json_object"}}
                return ChatOpenAI(**kwargs)
        except ImportError:
            logger.warning("[LangChainMixin] Could not import langchain_openai.")
            
        return None

class LangChainAgent(BaseAgent, LangChainMixin):
    """
    Convenience class that combines BaseAgent and LangChainMixin.
    Preserves existing inheritance structure for standard LangChain agents.
    """
    pass
