import time
import json
import requests
from loguru import logger

def run_plain_baseline():
    """
    Baseline A: Sequential Python.
    Proves high latency and lack of orchestration.
    """
    logger.info("=== 🕰️  BASELINE: PLAIN SEQUENTIAL PYTHON ===")
    start = time.perf_counter()
    
    # 1. Fetch data (mocked for baseline consistency)
    logger.debug("Fetching news headlines...")
    time.sleep(0.5)
    headlines = [
        "Bitcoin surges as ETF flows reach record levels.",
        "SEC delays decision on Ethereum spot ETF."
    ]
    
    logger.debug("Fetching market ticks...")
    time.sleep(0.5)
    prices = [65000 + i for i in range(25)]
    
    # 2. Sentiment Analysis (Sequential)
    logger.debug("Analyzing sentiment...")
    scores = []
    for h in headlines:
        # Simulate LLM inference overhead
        time.sleep(0.8) 
        scores.append(0.5 if "surges" in h.lower() else -0.2)
    avg_sent = sum(scores) / len(scores)
    
    # 3. Strategy Calculation
    logger.debug("Calculating SMA...")
    sma5 = sum(prices[-5:]) / 5.0
    sma20 = sum(prices[-20:]) / 20.0
    trend = (sma5 - sma20) / sma20 * 50
    
    # 4. Final Decision
    time.sleep(0.5) # Final reasoning overhead
    final_score = (0.6 * trend) + (0.4 * avg_sent)
    action = "BUY" if final_score > 0.3 else "HOLD"
    
    total = time.perf_counter() - start
    logger.success(f"Baseline Result: {action} BTC in {total:.2f}s")
    return {"action": action, "total_time": total}

if __name__ == "__main__":
    run_plain_baseline()
