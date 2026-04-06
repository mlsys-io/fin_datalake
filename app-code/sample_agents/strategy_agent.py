from __future__ import annotations

from typing import Any, Dict, List
import json
import traceback

from loguru import logger

from etl.agents.base import BaseAgent
from etl.agents.context import get_context
from etl.agents.langchain_adapter import LangChainMixin


class StrategyAgent(BaseAgent, LangChainMixin):
    """
    Fusion/decision agent that combines market-state metrics with delegated
    market-news analysis to produce a trading signal.
    """

    CAPABILITIES = ["strategy", "trading_signal"]

    def build_executor(self):
        def _execute(payload: Dict[str, Any]) -> Dict[str, Any]:
            symbol = str(payload.get("symbol", "UNKNOWN")).upper()
            headlines = self._extract_headlines(payload)

            logger.info(f"[StrategyAgent] Analyzing signal for {symbol}...")
            ohlc_data = self._resolve_data(payload)
            market_state = self._resolve_market_state(payload, ohlc_data)
            trend_score = self._resolve_trend_score(market_state, ohlc_data)
            logger.debug(f"[StrategyAgent] Market trend score: {trend_score:.2f}")

            analyst_result = {
                "label": "neutral",
                "score": 0.0,
                "summary": "No headline analysis available.",
                "headlines": [],
            }
            if headlines:
                logger.debug(f"[StrategyAgent] Delegating market-news analysis for {len(headlines)} headlines...")
                try:
                    response = self.delegate(
                        "market_news_analysis",
                        {
                            "symbol": symbol,
                            "headlines": headlines,
                            "market_state": market_state,
                        },
                    )
                    if isinstance(response, dict):
                        analyst_result = self._normalize_analyst_result(response)
                except Exception as exc:
                    logger.error(f"[StrategyAgent] Market analyst delegation failed: {exc}")

            sentiment_label = analyst_result["label"]
            sentiment_score = float(analyst_result["score"])
            llm = self._get_llm(temperature=0.1, json_mode=True)

            if llm:
                logger.debug("[StrategyAgent] Using configured LLM for final signal generation.")
                action, confidence = self._generate_signal_llm(
                    symbol=symbol,
                    trend_score=trend_score,
                    sentiment_label=sentiment_label,
                    sentiment_score=sentiment_score,
                    market_state=market_state,
                    analyst_summary=analyst_result.get("summary", ""),
                    llm=llm,
                )
            else:
                logger.debug("[StrategyAgent] No LLM available. Using programmatic fallback.")
                action, confidence = self._generate_signal_heuristic(trend_score, sentiment_score)

            result = {
                "symbol": symbol,
                "action": action,
                "confidence": round(confidence, 2),
                "trend_score": round(trend_score, 2),
                "sentiment_label": sentiment_label,
                "sentiment_score": round(sentiment_score, 2),
                "analyst_summary": analyst_result.get("summary", ""),
                "analyst_headlines": analyst_result.get("headlines", []),
                "market_state": market_state,
                "timestamp_ms": self._get_current_ms(),
            }

            self._publish_to_context(symbol, result)
            logger.success(f"[StrategyAgent] Signal emitted: {action} {symbol} ({confidence:.0%} conf)")
            return result

        return _execute

    def _extract_headlines(self, payload: Dict[str, Any]) -> List[str]:
        headlines = payload.get("headlines", [])
        if isinstance(headlines, list):
            return [str(item).strip() for item in headlines if str(item).strip()]
        if headlines:
            return [str(headlines).strip()]
        return []

    def _normalize_analyst_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        label = str(result.get("label", "neutral")).lower()
        if label not in {"bullish", "bearish", "neutral"}:
            label = "neutral"
        try:
            score = float(result.get("score", 0.0))
        except (TypeError, ValueError):
            score = 0.0

        breakdown = result.get("headlines", [])
        if not isinstance(breakdown, list):
            breakdown = []

        return {
            "label": label,
            "score": min(max(score, -1.0), 1.0),
            "summary": str(result.get("summary", "")).strip(),
            "headlines": breakdown,
        }

    def _generate_signal_heuristic(self, trend_score: float, sentiment_score: float):
        final_score = (0.65 * trend_score) + (0.35 * sentiment_score)

        if final_score > 0.3:
            action = "BUY"
        elif final_score < -0.3:
            action = "SELL"
        else:
            action = "HOLD"

        confidence = min(abs(final_score), 1.0)
        return action, confidence

    def _generate_signal_llm(
        self,
        *,
        symbol: str,
        trend_score: float,
        sentiment_label: str,
        sentiment_score: float,
        market_state: Dict[str, Any],
        analyst_summary: str,
        llm: Any,
    ):
        try:
            from langchain_core.messages import HumanMessage, SystemMessage

            sys_msg = SystemMessage(content=f"You are an expert quantitative trading AI for {symbol}.")
            prompt = (
                "Integrate the technical market state and news analysis into a final trading decision.\n\n"
                f"Symbol: {symbol}\n"
                f"Trend Score (-1.0 to 1.0): {trend_score:.2f}\n"
                f"Sentiment Label: {sentiment_label}\n"
                f"Sentiment Score (-1.0 to 1.0): {sentiment_score:.2f}\n"
                f"Market State: {json.dumps(market_state, default=str)}\n"
                f"Analyst Summary: {analyst_summary}\n\n"
                "Return valid JSON with exactly these keys:\n"
                "- action: BUY, SELL, or HOLD\n"
                "- confidence: float between 0.0 and 1.0\n"
            )

            response = llm.invoke([sys_msg, HumanMessage(content=prompt)])
            content = response.content if hasattr(response, "content") else str(response)
            parsed = json.loads(content)

            action = str(parsed.get("action", "HOLD")).upper()
            if action not in ["BUY", "SELL", "HOLD"]:
                action = "HOLD"
            confidence = float(parsed.get("confidence", 0.0))
            return action, min(max(confidence, 0.0), 1.0)
        except Exception as exc:
            logger.error(f"[StrategyAgent] LLM signal failed: {exc}. Falling back to heuristic.")
            return self._generate_signal_heuristic(trend_score, sentiment_score)

    def _resolve_data(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        if "data_ref" in payload:
            import ray

            try:
                logger.info("[StrategyAgent] Resolving Zero-Copy ObjectRef...")
                arrow_table = ray.get(payload["data_ref"])
                return arrow_table.to_pylist()
            except Exception as exc:
                logger.error(f"[StrategyAgent] Zero-copy resolution failed: {exc}")
                traceback.print_exc()
                return []

        return payload.get("ohlc_data", [])

    def _resolve_market_state(self, payload: Dict[str, Any], data: List[Dict[str, Any]]) -> Dict[str, Any]:
        market_state = payload.get("market_state")
        if isinstance(market_state, dict) and market_state:
            resolved = dict(market_state)
            resolved.setdefault("trend_score", self._compute_trend_score_from_state(resolved))
            return resolved
        return self._compute_market_state_from_ohlc(data)

    def _compute_market_state_from_ohlc(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        closes: List[float] = []
        volumes: List[float] = []
        timestamps: List[Any] = []
        for row in data:
            try:
                closes.append(float(row.get("close")))
            except (TypeError, ValueError):
                continue
            try:
                volumes.append(float(row.get("volume") or 0.0))
            except (TypeError, ValueError):
                volumes.append(0.0)
            timestamps.append(row.get("timestamp"))

        if not closes:
            return {"trend_score": 0.0}

        sma_5 = sum(closes[-5:]) / min(len(closes), 5) if closes else None
        sma_20 = sum(closes[-20:]) / min(len(closes), 20) if closes else None
        avg_price = sum(closes) / len(closes)
        avg_volume = sum(volumes) / len(volumes) if volumes else 0.0
        total_volume = sum(volumes)
        vwap = (sum(price * volume for price, volume in zip(closes, volumes)) / total_volume) if total_volume > 0 else None
        first_price = closes[0]
        last_price = closes[-1]
        return_pct = ((last_price - first_price) / first_price * 100.0) if first_price else None
        volatility = None
        if len(closes) >= 2:
            variance = sum((price - avg_price) ** 2 for price in closes) / len(closes)
            volatility = variance ** 0.5

        state = {
            "window_count": len(closes),
            "last_price": last_price,
            "avg_price": avg_price,
            "avg_volume": avg_volume,
            "min_price": min(closes),
            "max_price": max(closes),
            "price_return_pct": return_pct,
            "volatility_estimate": volatility,
            "vwap": vwap,
            "sma_5": sma_5,
            "sma_20": sma_20,
            "last_tick_at": timestamps[-1] if timestamps else None,
        }
        state["trend_score"] = self._compute_trend_score_from_state(state)
        return state

    def _compute_trend_score_from_state(self, market_state: Dict[str, Any]) -> float:
        try:
            sma_5 = float(market_state.get("sma_5"))
            sma_20 = float(market_state.get("sma_20"))
            if sma_20 != 0:
                pct_diff = (sma_5 - sma_20) / sma_20
                return max(min(pct_diff * 50.0, 1.0), -1.0)
        except (TypeError, ValueError):
            pass

        try:
            return_pct = float(market_state.get("price_return_pct"))
            return max(min(return_pct / 5.0, 1.0), -1.0)
        except (TypeError, ValueError):
            return 0.0

    def _resolve_trend_score(self, market_state: Dict[str, Any], data: List[Dict[str, Any]]) -> float:
        try:
            return float(market_state.get("trend_score"))
        except (TypeError, ValueError):
            return float(self._compute_market_state_from_ohlc(data).get("trend_score", 0.0))

    def _publish_to_context(self, symbol: str, signal: Dict[str, Any]):
        try:
            store = get_context()
            key = f"signal:{symbol}"
            store.set(key, signal, ttl=300)
            logger.debug(f"[StrategyAgent] Saved signal to ContextStore under {key}")
        except Exception as exc:
            logger.error(f"[StrategyAgent] Failed to write to ContextStore: {exc}")

    def _get_current_ms(self) -> int:
        import time

        return int(time.time() * 1000)
