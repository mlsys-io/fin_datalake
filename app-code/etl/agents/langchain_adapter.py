from typing import Any
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
        Dynamically initializes a LangChain ChatOpenAI model.
        Requires OPENAI_API_KEY in the environment.
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

    def _get_gemini_llm(self, model_name: str = "gemini-2.0-flash", temperature: float = 0.0) -> Any:
        """
        Dynamically initialises a LangChain-compatible Gemini ChatModel.
        Requires GOOGLE_API_KEY in the environment.

        Args:
            model_name: Gemini model identifier (e.g. "gemini-2.0-flash", "gemini-1.5-pro").
            temperature: Sampling temperature (0.0 = deterministic).

        Returns:
            A ChatGoogleGenerativeAI instance, or None if the key/package is missing.
        """
        try:
            from langchain_google_genai import ChatGoogleGenerativeAI
            api_key = os.environ.get("GOOGLE_API_KEY")

            if api_key and api_key.lower() not in ["dummy", "test", "demo"]:
                return ChatGoogleGenerativeAI(
                    model=model_name,
                    temperature=temperature,
                    google_api_key=api_key,
                )
            else:
                logger.warning("[LangChainMixin] GOOGLE_API_KEY not set or is a placeholder.")
        except ImportError:
            logger.warning("[LangChainMixin] langchain-google-genai not installed. Run: uv add langchain-google-genai")

        return None


class LangChainAgent(BaseAgent, LangChainMixin):
    """
    Convenience class that combines BaseAgent and LangChainMixin.
    Preserves existing inheritance structure for standard LangChain agents.
    """
    pass
