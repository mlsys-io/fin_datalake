from typing import Any, Dict

from etl.agents.base import BaseAgent


class SentimentAgent(BaseAgent):
    """
    Minimal SentimentAgent implementation used by Overseer scaling policies.
    """

    CAPABILITIES = ["sentiment"]

    def build_executor(self):
        def _analyze(payload: Any) -> Dict[str, Any]:
            return {"sentiment": "neutral", "input": payload}

        return _analyze
