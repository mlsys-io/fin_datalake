from __future__ import annotations

from typing import Any, Dict, List
import json

from loguru import logger

from etl.agents.langchain_adapter import LangChainAgent


class MarketAnalystAgent(LangChainAgent):
    """Structured market-news analysis agent used by StrategyAgent."""

    CAPABILITIES = ["market_news_analysis"]

    def build_executor(self):
        llm = self._get_llm(model_name="gemini-2.5-flash", temperature=0.0, json_mode=True)

        def _execute(payload: Any) -> Dict[str, Any]:
            headlines = self._extract_headlines(payload)
            market_state = payload.get("market_state", {}) if isinstance(payload, dict) else {}
            if not headlines:
                return {
                    "label": "neutral",
                    "score": 0.0,
                    "summary": "No headlines were provided for market analysis.",
                    "headlines": [],
                }

            if llm:
                try:
                    return self._analyze_with_llm(headlines, market_state, llm)
                except Exception as exc:
                    logger.error(f"[MarketAnalystAgent] LLM analysis failed: {exc}. Using heuristic fallback.")

            return self._heuristic_analysis(headlines)

        return _execute

    def _extract_headlines(self, payload: Any) -> List[str]:
        if isinstance(payload, str):
            return [payload]

        if isinstance(payload, list):
            return [str(item).strip() for item in payload if str(item).strip()]

        if isinstance(payload, dict):
            headlines = payload.get("headlines", [])
            if isinstance(headlines, list):
                return [str(item).strip() for item in headlines if str(item).strip()]
            if headlines:
                return [str(headlines).strip()]

        return [str(payload).strip()] if payload else []

    def _heuristic_analysis(self, headlines: List[str]) -> Dict[str, Any]:
        positive_words = {"surges", "jumps", "soars", "bullish", "growth", "profit", "beats", "rally", "inflows", "approval"}
        negative_words = {"plummets", "drops", "falls", "bearish", "loss", "misses", "crash", "lawsuit", "hack", "outflows"}

        breakdown = []
        scores: List[float] = []
        for headline in headlines:
            words = set(headline.lower().split())
            pos_count = len(words.intersection(positive_words))
            neg_count = len(words.intersection(negative_words))

            score = 0.0
            label = "neutral"
            if pos_count > neg_count:
                score = min(0.3 * (pos_count - neg_count), 0.9)
                label = "bullish"
            elif neg_count > pos_count:
                score = max(-0.3 * (neg_count - pos_count), -0.9)
                label = "bearish"

            scores.append(score)
            breakdown.append(
                {
                    "headline": headline,
                    "label": label,
                    "score": round(score, 2),
                }
            )

        aggregate_score = sum(scores) / len(scores) if scores else 0.0
        aggregate_label = "neutral"
        if aggregate_score > 0.15:
            aggregate_label = "bullish"
        elif aggregate_score < -0.15:
            aggregate_label = "bearish"

        return {
            "label": aggregate_label,
            "score": round(aggregate_score, 2),
            "summary": f"Headline sentiment is {aggregate_label} based on {len(headlines)} recent articles.",
            "headlines": breakdown,
        }

    def _analyze_with_llm(self, headlines: List[str], market_state: Dict[str, Any], llm: Any) -> Dict[str, Any]:
        from langchain_core.messages import HumanMessage, SystemMessage

        prompt = (
            "Analyze the supplied market headlines and return JSON.\n\n"
            f"Headlines: {json.dumps(headlines[:8])}\n"
            f"Market context: {json.dumps(market_state, default=str)}\n\n"
            "Return valid JSON with exactly these keys:\n"
            '- label: "bullish", "bearish", or "neutral"\n'
            "- score: float between -1.0 and 1.0\n"
            "- summary: one short sentence\n"
            "- headlines: list of objects with headline, label, score\n"
        )
        response = llm.invoke(
            [
                SystemMessage(content="You are a precise market-news analyst. Respond only with valid JSON."),
                HumanMessage(content=prompt),
            ]
        )
        content = response.content if hasattr(response, "content") else str(response)
        parsed = json.loads(content)
        label = str(parsed.get("label", "neutral")).lower()
        if label not in {"bullish", "bearish", "neutral"}:
            label = "neutral"
        score = float(parsed.get("score", 0.0))
        summary = str(parsed.get("summary", "")).strip() or f"Headline sentiment is {label}."
        raw_breakdown = parsed.get("headlines", [])
        breakdown = []
        if isinstance(raw_breakdown, list):
            for item in raw_breakdown[: len(headlines)]:
                if not isinstance(item, dict):
                    continue
                item_label = str(item.get("label", "neutral")).lower()
                if item_label not in {"bullish", "bearish", "neutral"}:
                    item_label = "neutral"
                breakdown.append(
                    {
                        "headline": str(item.get("headline", "")),
                        "label": item_label,
                        "score": round(float(item.get("score", 0.0)), 2),
                    }
                )

        return {
            "label": label,
            "score": round(min(max(score, -1.0), 1.0), 2),
            "summary": summary,
            "headlines": breakdown,
        }


def deploy_analyst() -> Any:
    return MarketAnalystAgent.deploy(name="MarketAnalyst", num_cpus=1)
