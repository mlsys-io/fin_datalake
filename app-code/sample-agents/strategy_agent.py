from typing import Any, Dict, List
from loguru import logger
import json
import traceback

from etl.agents.base import BaseAgent
from etl.agents.context import get_context_store
from etl.agents.langchain_adapter import LangChainMixin

class StrategyAgent(BaseAgent, LangChainMixin):
    """
    Computes trading signals based on technical indicators (SMA) 
    and sentiment analysis (delegated to SentimentAgent).
    
    Demonstrates: Zero-Copy data ingestion, Agent-to-Agent Delegation, ContextStore sharing.
    """

    CAPABILITIES = ["strategy", "trading_signal"]

    def build_executor(self):
        def _execute(payload: Dict[str, Any]) -> Dict[str, Any]:
            """
            Expects payload format:
            {
                "symbol": "BTCUSD",
                "ohlc_data": [...],  # OR "data_ref": <Ray ObjectRef> for Zero-Copy
                "headlines": ["...", "..."]
            }
            """
            symbol = payload.get("symbol", "UNKNOWN")
            headlines = payload.get("headlines", [])
            
            logger.info(f"[StrategyAgent] Analyzing signal for {symbol}...")
            
            # --- 1. Zero-Copy or Standard Data Access ---
            ohlc_data = self._resolve_data(payload)
            
            # --- 2. Technical Analysis (SMA 5 vs 20) ---
            trend_score = self._compute_sma_trend(ohlc_data)
            logger.debug(f"[StrategyAgent] Computed trend score: {trend_score:.2f}")
            
            # --- 3. Sentiment Delegation ---
            avg_sentiment_score = 0.0
            if headlines:
                logger.debug(f"[StrategyAgent] Delegating sentiment analysis for {len(headlines)} headlines...")
                try:
                    sentiment_response = self.delegate("sentiment", headlines)
                    if sentiment_response and isinstance(sentiment_response, list):
                        scores = [item.get("score", 0.0) for item in sentiment_response]
                        if scores:
                            avg_sentiment_score = sum(scores) / len(scores)
                except Exception as e:
                    logger.error(f"[StrategyAgent] Sentiment delegation failed: {e}")
            
            # --- 4. Signal Generation (LLM or Heuristic) ---
            llm = self._get_llm(temperature=0.1, json_mode=True)
            
            if llm:
                logger.debug("[StrategyAgent] Using LLM for final signal generation.")
                action, confidence = self._generate_signal_llm(symbol, trend_score, avg_sentiment_score, headlines, llm)
            else:
                logger.debug("[StrategyAgent] No LLM API key found. Using programmatic fallback.")
                action, confidence = self._generate_signal_heuristic(trend_score, avg_sentiment_score)
            
            result = {
                "symbol": symbol,
                "action": action,
                "confidence": round(confidence, 2),
                "trend_score": round(trend_score, 2),
                "sentiment_score": round(avg_sentiment_score, 2),
                "timestamp_ms": self._get_current_ms()
            }
            
            # --- 5. ContextStore Publish ---
            self._publish_to_context(symbol, result)
            
            logger.success(f"[StrategyAgent] Signal emitted: {action} {symbol} ({confidence:.0%} conf)")
            return result

        return _execute

    def _generate_signal_heuristic(self, trend_score: float, sentiment_score: float):
        """Fallback programmatic signal generation."""
        # 60% Tech, 40% Sentiment
        final_score = (0.6 * trend_score) + (0.4 * sentiment_score)
        
        if final_score > 0.3:
            action = "BUY"
        elif final_score < -0.3:
            action = "SELL"
        else:
            action = "HOLD"
            
        confidence = min(abs(final_score), 1.0)
        return action, confidence

    def _generate_signal_llm(self, symbol: str, trend_score: float, sentiment_score: float, headlines: List[str], llm: Any):
        """Primary LLM-based signal generation using LangChain."""
        try:
            from langchain_core.messages import SystemMessage, HumanMessage
            
            sys_msg = SystemMessage(content=f"You are an expert quantitative trading AI for {symbol}.")
            
            prompt = (
                f"Your task is to integrate a technical moving average trend score and a fundamental news sentiment score "
                f"to output a final trading decision.\n\n"
                f"Data given:\n"
                f"- Technical Trend Score (-1.0 to 1.0): {trend_score:.2f} (Negative is bearish crossover, Positive is bullish)\n"
                f"- Market Sentiment Score (-1.0 to 1.0): {sentiment_score:.2f} (Negative is bad news, Positive is good news)\n"
                f"- Recent News Headlines: {json.dumps(headlines[:3])}\n\n"
                "Provide your output as a JSON object with strictly these two keys:\n"
                "- 'action': exactly one of 'BUY', 'SELL', or 'HOLD'.\n"
                "- 'confidence': a float between 0.0 and 1.0 representing your conviction.\n"
            )
            
            response = llm.invoke([sys_msg, HumanMessage(content=prompt)])
            
            content = response.content
            parsed = json.loads(content)
            
            action = str(parsed.get("action", "HOLD")).upper()
            if action not in ["BUY", "SELL"]:
                action = "HOLD"
            confidence = float(parsed.get("confidence", 0.0))
            
            return action, min(max(confidence, 0.0), 1.0)
            
        except Exception as e:
            logger.error(f"[StrategyAgent] LLM signal failed: {e}. Falling back to heuristic.")
            return self._generate_signal_heuristic(trend_score, sentiment_score)

    def _resolve_data(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Handles both Zero-Copy ObjectRefs and raw JSON data."""
        if "data_ref" in payload:
            import ray
            try:
                logger.info("[StrategyAgent] Resolving Zero-Copy ObjectRef...")
                arrow_table = ray.get(payload["data_ref"])
                # Convert back to dicts for simplicity in SMA calculation
                return arrow_table.to_pylist()
            except Exception as e:
                logger.error(f"[StrategyAgent] Zero-copy resolution failed: {e}")
                traceback.print_exc()
                return []
        
        return payload.get("ohlc_data", [])

    def _compute_sma_trend(self, data: List[Dict[str, Any]]) -> float:
        """
        Calculates SMA5 and SMA20 over the 'close' price.
        Returns a score from -1.0 to 1.0 based on crossover distance.
        """
        if not data or len(data) < 20:
            return 0.0
            
        closes = []
        for row in data:
            val = row.get("close")
            if val is not None:
                try:
                    closes.append(float(val))
                except ValueError:
                    pass
                    
        if len(closes) < 20:
            return 0.0
            
        # Recent data counts more (assuming time series is in chronological order, older first)
        sma5 = sum(closes[-5:]) / 5.0
        sma20 = sum(closes[-20:]) / 20.0
        
        # % diff mapping to a -1.0 to 1.0 scale
        pct_diff = (sma5 - sma20) / sma20
        # If SMA5 is 2% higher than SMA20, that's strongly bullish (score ~1.0)
        # 0.02 * 50 = 1.0
        score = pct_diff * 50.0 
        
        return max(min(score, 1.0), -1.0)

    def _publish_to_context(self, symbol: str, signal: Dict[str, Any]):
        """Persist to the ContextStore for the MCP proxy to read."""
        try:
            store = get_context_store()
            key = f"signal:{symbol}"
            store.set(key, signal, ttl=300) # 5 min expiry
            logger.debug(f"[StrategyAgent] Saved signal to ContextStore under {key}")
        except Exception as e:
            logger.error(f"[StrategyAgent] Failed to write to ContextStore: {e}")

    def _get_current_ms(self) -> int:
        import time
        return int(time.time() * 1000)
