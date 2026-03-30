import time
import asyncio
from loguru import logger
import ray
import ray.serve as serve

from etl.agents.sentiment_agent import SentimentAgent
from sample_agents.strategy_agent import StrategyAgent
from etl.agents.hub import get_hub
from etl.agents.context import get_context_store
from pipelines.market_pulse_ingest import market_pulse_ingest

def run_demo():
    """
    Master flow for the 30-second Demo.
    Proves: Shared Infrastructure (Ray), Sub-Agent Delegation, and ContextStore routing.
    """
    logger.info("\n" + "="*50 + "\n🚀 STARTING MARKET PULSE: UNIFIED FABRIC DEMO\n" + "="*50)
    start_time = time.perf_counter()
    
    # --- 1. Infrastructure Setup ---
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)
    hub = get_hub()
    
    # --- 2. Deploy Agents ---
    logger.info("\n[Step 1] Deploying AI Agents to Ray Cluster...")
    # NOTE: SentimentAgent *must* be deployed first so StrategyAgent can delegate to it
    SentimentAgent.deploy(name="MarketSentiment-1")
    StrategyAgent.deploy(name="CryptoStrategy-1")
    
    # Verify registration 
    time.sleep(2)  # Give Ray Serve a moment to stabilize
    registered = ray.get(hub.find_by_capability.remote("strategy"))
    logger.success(f"Agents deployed. Strategy capability holders: {registered}")
    
    # --- 3. Run Ingestion Pipeline (Prefect + I/O Adapters) ---
    logger.info("\n[Step 2] Triggering Data Lake Ingestion (Prefect Flow)...")
    ingest_result = market_pulse_ingest()
    
    # Extract data from the pipeline result (if it failed, we use fallback data)
    headlines = [item.get("headline", "") for item in ingest_result.get("news", [])]
    ohlc_data = ingest_result.get("ohlc", [])
    
    # Fallback if the API returns nothing
    if not headlines:
        headlines = ["Bitcoin ETF approved by SEC, driving massive inflows.", "Markets rally on rate cut hopes."]
        logger.warning("Using fallback news headlines.")
    if not ohlc_data:
        ohlc_data = [{"close": 65000+i, "volume": 1.5} for i in range(25)]
        
    # --- 4. Agent Execution (The "Brain") ---
    logger.info(f"\n[Step 3] Dispatching data to StrategyAgent (analyzing {len(ohlc_data)} ticks, {len(headlines)} headlines)")
    
    # Get the strategy agent's Serve handle via capability lookup + serve.get_app_handle
    strategy_names = ray.get(hub.find_by_capability.remote("strategy"))
    if not strategy_names:
        logger.error("Failed to find StrategyAgent in registry!")
        return
    strategy_handle = serve.get_app_handle(strategy_names[0])

    # Trigger the agent!
    # The StrategyAgent will internally delegate to the SentimentAgent!
    payload = {
        "symbol": "BTCUSD",
        "ohlc_data": ohlc_data,
        "headlines": headlines
    }
    
    # DeploymentHandle returns an async future; run it synchronously
    logger.info("Executing Agent Workflow (waiting on LLM...)")
    agent_start = time.perf_counter()
    signal_result = asyncio.run(strategy_handle.ask(payload))
    agent_time = time.perf_counter() - agent_start
    
    # --- 5. Verify ContextStore (Gateway exposure) ---
    logger.info("\n[Step 4] Verifying ContextStore publication for the MCP server...")
    ctx = get_context_store()
    saved_signal = ctx.get("signal:BTCUSD")
    
    # --- 6. The "Showmanship" Conclusion ---
    total_time = time.perf_counter() - start_time
    logger.info("\n" + "="*50)
    logger.info(f"🎯 PIPELINE COMPLETE IN {total_time:.2f}s (Agent inference: {agent_time:.2f}s)")
    logger.info("="*50)
    
    if signal_result and saved_signal:
        action = signal_result.get("action", "HOLD")
        conf = signal_result.get("confidence", 0.0) * 100
        tech = signal_result.get("trend_score", 0.0)
        sent = signal_result.get("sentiment_score", 0.0)
        
        # Colorize terminal output based on action
        color = "\033[92m" if action == "BUY" else "\033[91m" if action == "SELL" else "\033[93m"
        reset = "\033[0m"
        
        print(f"\n{color}🔥 LIVE SIGNAL: {action} BTCUSD — {conf:.0f}% Confidence{reset}")
        print(f"   ↳ Trend (SMA): {tech:.2f}")
        print(f"   ↳ Sentiment (LLM): {sent:.2f}")
        logger.success("Signal successfully published to ContextStore for external clients.")
    else:
        logger.error("Failed to generate or persist signal.")

if __name__ == "__main__":
    # Ray behaves cleaner functionally in the local isolated context when started directly
    # To run this script: `uv run python -m pipelines.market_pulse_demo`
    import httpx
    # Disable timeout limits for the LLM inference when routing via Ray Serve HTTP
    try:
        run_demo()
    except KeyboardInterrupt:
        logger.info("Demo interrupted.")
    finally:
        # Give logs a moment to flush
        time.sleep(1)
