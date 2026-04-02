"""
Dummy agents for exercising the typed capability registry and delegation paths.
"""

from __future__ import annotations

from typing import Any

from etl.agents.base import BaseAgent


def _latest_user_message(payload: Any) -> str:
    if isinstance(payload, list):
        for item in reversed(payload):
            if isinstance(item, dict) and item.get("role") == "user":
                return str(item.get("content", "")).strip()
        return str(payload[-1]) if payload else ""
    if isinstance(payload, dict):
        return str(payload.get("content") or payload.get("message") or payload).strip()
    return str(payload).strip()


class SupportAgent(BaseAgent):
    CAPABILITY_SPECS = [
        {
            "id": "chat.support.respond",
            "description": "Conversational support assistant for operational questions.",
            "tags": ["llm", "chat", "support"],
            "aliases": ["chat", "support"],
            "input_type": "text",
            "output_type": "text",
            "interaction_mode": "chat",
        }
    ]

    def build_executor(self):
        def _execute(payload: Any) -> dict[str, Any]:
            prompt = _latest_user_message(payload)
            return {
                "content": (
                    "SupportAgent ready.\n"
                    f"- Issue summary: {prompt or 'No issue provided'}\n"
                    "- Suggested checks: connectivity, input payload, and downstream service health."
                )
            }

        return _execute


class SentimentModelAgent(BaseAgent):
    CAPABILITY_SPECS = [
        {
            "id": "nlp.sentiment.score",
            "description": "Scores short financial text as bullish, bearish, or neutral.",
            "tags": ["ml", "nlp", "classification", "finance"],
            "aliases": ["sentiment", "news_analysis"],
            "input_type": "text",
            "output_type": "sentiment_scores",
            "interaction_mode": "invoke",
        }
    ]

    def build_executor(self):
        positive_words = {"surge", "surges", "growth", "beat", "beats", "bullish", "rally", "gain"}
        negative_words = {"drop", "drops", "miss", "misses", "bearish", "hack", "crash", "loss"}

        def _execute(payload: Any) -> list[dict[str, Any]]:
            text = _latest_user_message(payload).lower()
            words = set(text.replace(".", " ").replace(",", " ").split())
            pos = len(words.intersection(positive_words))
            neg = len(words.intersection(negative_words))

            sentiment = "neutral"
            score = 0.0
            if pos > neg:
                sentiment = "bullish"
                score = min(0.25 * (pos - neg), 0.9)
            elif neg > pos:
                sentiment = "bearish"
                score = max(-0.25 * (neg - pos), -0.9)

            return [{"headline": text or "n/a", "sentiment": sentiment, "score": round(score, 2)}]

        return _execute


class ForecastAgent(BaseAgent):
    CAPABILITY_SPECS = [
        {
            "id": "timeseries.forecast.generate",
            "description": "Generates a simple next-step forecast from numeric observations.",
            "tags": ["ml", "forecasting", "timeseries"],
            "aliases": ["forecast", "prediction"],
            "input_type": "timeseries",
            "output_type": "forecast",
            "interaction_mode": "invoke",
        }
    ]

    def build_executor(self):
        def _execute(payload: Any) -> dict[str, Any]:
            if isinstance(payload, dict):
                values = payload.get("values", [])
            else:
                values = []

            numeric_values = []
            for value in values:
                try:
                    numeric_values.append(float(value))
                except (TypeError, ValueError):
                    continue

            if len(numeric_values) < 2:
                next_value = numeric_values[-1] if numeric_values else 0.0
            else:
                slope = numeric_values[-1] - numeric_values[-2]
                next_value = numeric_values[-1] + slope

            return {
                "model": "dummy-linear-forecast",
                "input_points": len(numeric_values),
                "next_value": round(float(next_value), 2),
            }

        return _execute


class RouterAgent(BaseAgent):
    CAPABILITY_SPECS = [
        {
            "id": "orchestration.route.task",
            "description": "Routes tasks either by explicit target or typed capability.",
            "tags": ["orchestration", "routing"],
            "aliases": ["routing", "coordination"],
            "input_type": "object",
            "output_type": "object",
            "interaction_mode": "invoke",
        }
    ]

    def build_executor(self):
        def _execute(payload: Any) -> dict[str, Any]:
            if not isinstance(payload, dict):
                return {"router": "RouterAgent", "error": "Expected object payload"}

            action = payload.get("action", "describe")

            if action == "direct_support":
                result = self.delegate_to("SupportAgent", payload.get("message", ""))
                return {"router": "RouterAgent", "mode": "direct", "target": "SupportAgent", "result": result}

            if action == "score_sentiment":
                result = self.delegate(
                    "nlp.sentiment.score",
                    payload.get("text", ""),
                    required_tags=["ml", "classification"],
                    input_type="text",
                    output_type="sentiment_scores",
                    selection_strategy="first",
                )
                return {"router": "RouterAgent", "mode": "capability", "capability": "nlp.sentiment.score", "result": result}

            if action == "forecast":
                result = self.delegate(
                    "timeseries.forecast.generate",
                    {"values": payload.get("values", [])},
                    required_tags=["ml", "timeseries"],
                    input_type="timeseries",
                    output_type="forecast",
                    selection_strategy="first",
                )
                return {
                    "router": "RouterAgent",
                    "mode": "capability",
                    "capability": "timeseries.forecast.generate",
                    "result": result,
                }

            return {
                "router": "RouterAgent",
                "available_actions": ["direct_support", "score_sentiment", "forecast"],
            }

        return _execute
