from typing import Any, Dict, List, Union
import os
import json
from loguru import logger

from etl.agents.langchain_adapter import LangChainAgent

class SentimentAgent(LangChainAgent):
    """
    Analyzes financial news headlines for sentiment.
    Inherits from LangChainAgent, returning a pure LangChain Runnable (LCEL).
    """

    CAPABILITIES = ["sentiment", "news_analysis"]

    def build_executor(self):
        """Returns a LangChain Runnable that processes the payload."""
        
        from langchain_core.runnables import RunnableLambda
        # 1. Input Extraction (RunnableLambda)
        def _extract_headlines(payload: Any) -> dict:
            headlines = []
            if isinstance(payload, str):
                headlines = [payload]
            elif isinstance(payload, list):
                if len(payload) > 0 and isinstance(payload[-1], dict) and "content" in payload[-1]:
                    headlines = [payload[-1].get("content", "")]
                else:
                    headlines = [str(item) for item in payload if item]
            elif isinstance(payload, dict):
                val = payload.get("headlines", [])
                if isinstance(val, list):
                    headlines = [str(item) for item in val if item]
                else:
                    headlines = [str(val)]
                    
            return {"headlines_json": json.dumps(headlines)}

        extract_step = RunnableLambda(_extract_headlines)
        
        # 2. Initialize LLM via Mixin
        llm = self._get_llm(temperature=0.0, json_mode=True)
        
        # If no real LLM is available, return the heuristic fallback
        if not llm:
            logger.warning("[SentimentAgent] No LLM available. Returning heuristic Runnable.")
            return extract_step | RunnableLambda(self._heuristic_fallback)

        # 3. Build the LLM Chain (LCEL)
        try:
            from langchain_core.prompts import PromptTemplate
            from langchain_core.output_parsers import JsonOutputParser
            from langchain_core.runnables import RunnableLambda
            prompt = PromptTemplate.from_template(
                "Analyze the following financial headlines for Bitcoin/Crypto.\n"
                "Return valid JSON as a list of objects exactly matching this structure:\n"
                "{{\"results\": [{{\"headline\": \"...\", \"sentiment\": \"...\", \"score\": 0.0}}]}}\n\n"
                "Headlines: {headlines_json}"
            )
            
            # Formatter to unwrap the {"results": [...]} JSON wrapper
            def _format_output(parsed_json: dict) -> List[Dict[str, Any]]:
                results = parsed_json.get("results", [])
                final_results = []
                for r in results:
                    final_results.append({
                        "headline": str(r.get("headline", "")),
                        "sentiment": str(r.get("sentiment", "neutral")).lower(),
                        "score": float(r.get("score", 0.0))
                    })
                return final_results
                
            format_step = RunnableLambda(_format_output)

            # Assemble the LCEL Chain
            chain = extract_step | prompt | llm | JsonOutputParser() | format_step
            
            logger.info("[SentimentAgent] Successfully built LangChain LCEL Runnable.")
            return chain
            
        except Exception as e:
            logger.error(f"[SentimentAgent] Failed to build LLM chain: {e}. Using heuristic.")
            return extract_step | RunnableLambda(self._heuristic_fallback)

    def _heuristic_fallback(self, inputs: dict) -> List[Dict[str, Any]]:
        """Fallback keyword scoring used if the LLM chain cannot be built."""
        import json
        
        headlines = json.loads(inputs.get("headlines_json", "[]"))
        positive_words = {"surges", "jumps", "soars", "up", "bullish", "growth", "profit", "beats", "rally", "wins"}
        negative_words = {"plummets", "drops", "falls", "down", "bearish", "loss", "misses", "crash", "lawsuit", "hack"}
        
        results = []
        for h in headlines:
            words = set(h.lower().split())
            pos_count = len(words.intersection(positive_words))
            neg_count = len(words.intersection(negative_words))
            
            score = 0.0
            sentiment = "neutral"
            
            if pos_count > neg_count:
                score = min(0.3 * (pos_count - neg_count), 0.9)
                sentiment = "bullish"
            elif neg_count > pos_count:
                score = max(-0.3 * (neg_count - pos_count), -0.9)
                sentiment = "bearish"
                
            results.append({
                "headline": h,
                "sentiment": sentiment,
                "score": score
            })
            
        return results
