"""
GeminiAgent: A Ray Serve-deployed LLM Agent powered by Google Gemini.

Follows the identical BaseAgent pattern used by SentimentAgent and StrategyAgent:
    - Deployed via BaseAgent.deploy() → Ray Serve HTTP ingress
    - Auto-registered in AgentHub on startup
    - Reachable via POST /<app_name>/ask (handled by BaseAgent._bind_endpoints)
    - Falls back gracefully if GOOGLE_API_KEY is not set

Usage:
    >>> handle = GeminiAgent.deploy(name="GeminiAgent-1")
    >>> result = await handle.ask.remote({"question": "What is Delta Lake?"})

Environment:
    GOOGLE_API_KEY  - required for live Gemini inference
    GEMINI_MODEL    - optional override (default: gemini-2.0-flash)
"""

import os
from typing import Any

from etl.agents.base import BaseAgent
from etl.agents.langchain_adapter import LangChainMixin


class GeminiAgent(BaseAgent, LangChainMixin):
    """
    A general-purpose conversational agent backed by Google Gemini.

    Capabilities registered in AgentHub:
        - "gemini_chat"  — generic Q&A / conversational reasoning
        - "llm_qa"       — capability alias used by delegation lookups

    The executor is a simple LCEL chain:
        ChatPromptTemplate | ChatGoogleGenerativeAI | StrOutputParser

    Input to ask():
        - str  : raw question string (no session)
        - dict : {"question": "..."} (preferred for map/LCEL chain)
        - list : conversation history (when session_id is supplied via BaseAgent)

    Output:
        str — the model's plain-text response.
    """

    CAPABILITIES = ["gemini_chat", "llm_qa"]

    # The model can be overridden via environment variable for easy swapping.
    _DEFAULT_MODEL = "gemini-2.0-flash"

    def build_executor(self) -> Any:
        """
        Build and return the LCEL chain for Gemini inference.

        Chain:  ChatPromptTemplate → ChatGoogleGenerativeAI → StrOutputParser

        Falls back to an echo RunnableLambda when GOOGLE_API_KEY is absent,
        so the rest of the pipeline (AgentHub, Ray Serve ingress) can still
        be verified without a live API key.
        """
        from loguru import logger
        from langchain_core.runnables import RunnableLambda

        model_name = os.environ.get("GEMINI_MODEL", self._DEFAULT_MODEL)
        llm = self._get_gemini_llm(model_name=model_name, temperature=0.0)

        if not llm:
            # --- Fallback path: no API key or package missing ---
            logger.warning(
                "[GeminiAgent] No GOOGLE_API_KEY found or package missing. "
                "Using echo fallback. Set GOOGLE_API_KEY to enable live inference."
            )

            def _echo(payload: Any) -> str:
                question = _extract_question(payload)
                return f"[FALLBACK - no API key] Received: {question}"

            return RunnableLambda(_echo)

        # --- Live path: build LCEL chain ---
        try:
            from langchain_core.prompts import ChatPromptTemplate
            from langchain_core.output_parsers import StrOutputParser

            prompt = ChatPromptTemplate.from_messages([
                (
                    "system",
                    (
                        "You are a helpful AI assistant embedded in a distributed data lakehouse "
                        "built on Ray, Delta Lake, and MinIO. Be concise and accurate."
                    ),
                ),
                ("human", "{question}"),
            ])

            # Normalise heterogeneous inputs before the prompt step
            normalise = RunnableLambda(_normalise_input)
            chain = normalise | prompt | llm | StrOutputParser()

            logger.info(f"[GeminiAgent] LCEL chain built with model '{model_name}'.")
            return chain

        except Exception as e:
            from loguru import logger as _logger
            _logger.error(f"[GeminiAgent] Failed to build LCEL chain: {e}. Using echo fallback.")

            def _error_echo(payload: Any) -> str:
                return f"[ERROR] Chain build failed: {e}"

            return RunnableLambda(_error_echo)


# ---------------------------------------------------------------------------
# Private helpers — kept at module level so they are picklable by Ray
# ---------------------------------------------------------------------------

def _extract_question(payload: Any) -> str:
    """Pull a plain question string from arbitrary input shapes."""
    if isinstance(payload, str):
        return payload
    if isinstance(payload, dict):
        return payload.get("question", str(payload))
    if isinstance(payload, list) and payload:
        # Session history: last message is the user turn
        last = payload[-1]
        if isinstance(last, dict):
            return last.get("content", str(last))
    return str(payload)


def _normalise_input(payload: Any) -> dict:
    """
    Coerce any input shape into {"question": <str>} for the ChatPromptTemplate.

    Handles:
        str        → {"question": payload}
        dict       → pass-through if "question" key exists; else stringify
        list       → extracts last user message (conversation history / session mode)
    """
    if isinstance(payload, str):
        return {"question": payload}
    if isinstance(payload, dict) and "question" in payload:
        return payload
    if isinstance(payload, list) and payload:
        last = payload[-1]
        if isinstance(last, dict):
            return {"question": last.get("content", str(last))}
    return {"question": str(payload)}
