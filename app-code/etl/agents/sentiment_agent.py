from typing import Any, Dict

from etl.agents.base import BaseAgent


class SentimentAgent(BaseAgent):
    """
    Minimal SentimentAgent implementation used by Overseer scaling policies.
    """

    CAPABILITIES = ["sentiment"]

    def build_executor(self):
        def _analyze(payload: Any) -> Dict[str, Any]:
            # If session-based, payload is a list of messages. We take the last user message.
            if isinstance(payload, list) and len(payload) > 0:
                # Extract content from the most recent message
                text = payload[-1].get("content", str(payload))
            else:
                text = str(payload)
                
            return {"sentiment": "neutral", "input": text}

        return _analyze
