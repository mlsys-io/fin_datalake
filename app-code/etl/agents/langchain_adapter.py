from contextlib import contextmanager
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

    @contextmanager
    def _public_llm_ssl_env(self):
        """
        Use the public CA bundle for internet-facing LLM APIs without
        permanently overriding the MinIO/internal CA settings used elsewhere.
        """
        import certifi

        ca_file = str(os.environ.get("PUBLIC_CA_CERT") or certifi.where()).strip()
        keys = (
            "SSL_CERT_FILE",
            "REQUESTS_CA_BUNDLE",
            "CURL_CA_BUNDLE",
            "GRPC_DEFAULT_SSL_ROOTS_FILE_PATH",
        )
        previous = {key: os.environ.get(key) for key in keys}

        for key in keys:
            os.environ[key] = ca_file

        try:
            yield
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def _llm_invoke(self, llm: Any, payload: Any) -> Any:
        with self._public_llm_ssl_env():
            return llm.invoke(payload)

    def _get_llm(self, model_name: str | None = None, temperature: float = 0.0, json_mode: bool = False) -> Any:
        """
        Resolve a chat model with Gemini-first preference.

        Resolution order:
        1. ``LLM_PROVIDER`` / ``AGENT_LLM_PROVIDER`` explicit override
        2. Gemini when ``GOOGLE_API_KEY`` is available
        3. OpenAI when ``OPENAI_API_KEY`` is available
        """
        provider = str(
            os.environ.get("AGENT_LLM_PROVIDER")
            or os.environ.get("LLM_PROVIDER")
            or "gemini"
        ).strip().lower()

        preferred_model = str(
            model_name
            or os.environ.get("AGENT_LLM_MODEL")
            or os.environ.get("LLM_MODEL")
            or os.environ.get("GEMINI_MODEL")
            or "gemini-2.5-flash"
        ).strip()

        if provider in {"gemini", "google", "auto"}:
            llm = self._get_gemini_llm(
                model_name=preferred_model if provider != "auto" or preferred_model.startswith("gemini") else None,
                temperature=temperature,
                json_mode=json_mode,
            )
            if llm:
                return llm
            if provider != "auto":
                return None

        if provider in {"openai", "auto"}:
            fallback_model = preferred_model
            if fallback_model.startswith("gemini"):
                fallback_model = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
            llm = self._get_openai_llm(
                model_name=fallback_model,
                temperature=temperature,
                json_mode=json_mode,
            )
            if llm:
                return llm

        return None

    def _get_openai_llm(self, model_name: str = "gpt-4.1-mini", temperature: float = 0.0, json_mode: bool = False) -> Any:
        """Optional OpenAI fallback for teams that prefer that provider."""
        try:
            from langchain_openai import ChatOpenAI
            api_key = os.environ.get("OPENAI_API_KEY")

            if api_key and api_key.lower() not in ["dummy", "test", "demo"]:
                kwargs = {
                    "api_key": api_key,
                    "model": model_name,
                    "temperature": temperature,
                }
                if json_mode:
                    kwargs["model_kwargs"] = {"response_format": {"type": "json_object"}}
                return ChatOpenAI(**kwargs)
        except ImportError:
            logger.warning("[LangChainMixin] langchain_openai is not installed; skipping OpenAI fallback.")

        return None

    def _get_gemini_llm(self, model_name: str | None = None, temperature: float = 0.0, json_mode: bool = False) -> Any:
        """
        Dynamically initialises a LangChain-compatible Gemini ChatModel.
        Requires GOOGLE_API_KEY in the environment.

        Args:
            model_name: Gemini model identifier (e.g. "gemini-2.5-flash").
            temperature: Sampling temperature (0.0 = deterministic).

        Returns:
            A ChatGoogleGenerativeAI instance, or None if the key/package is missing.
        """
        try:
            from langchain_google_genai import ChatGoogleGenerativeAI
            api_key = os.environ.get("GOOGLE_API_KEY")
            resolved_model = str(model_name or os.environ.get("GEMINI_MODEL") or "gemini-2.5-flash").strip()

            if api_key and api_key.lower() not in ["dummy", "test", "demo"]:
                kwargs = {
                    "model": resolved_model,
                    "temperature": temperature,
                    "google_api_key": api_key,
                }
                if json_mode:
                    kwargs["convert_system_message_to_human"] = True
                return ChatGoogleGenerativeAI(**kwargs)
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
