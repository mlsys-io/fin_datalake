import time
import json
from loguru import logger

def run_airflow_sim_baseline():
    """
    Baseline B: Airflow-Equivalent.
    Simulates high overhead from task-to-task serialization and DB persistence.
    """
    logger.info("=== 🕰️  BASELINE: AIRFLOW-SIM (SERIALIZED TASKS) ===")
    start = time.perf_counter()
    
    # Step 1: Ingest (Source -> DB)
    logger.debug("TASK 1: Ingest (Serializing to JSON + Mock S3 latency)...")
    time.sleep(1.2) # High overhead for scheduler + I/O
    news_json = json.dumps(["Bitcoin surges ...", "..."])
    ohlc_json = json.dumps([65000, 65005])
    
    # Step 2: Sentiment Task (Read DB -> LLM -> Write DB)
    logger.debug("TASK 2: Sentiment (Deserializing -> Inference -> Reserializing)...")
    _ = json.loads(news_json)
    time.sleep(1.5) # Latency for another task container start
    sentiment_result = json.dumps({"score": 0.4})
    
    # Step 3: Strategy Task (Read DB x2 -> Cross Join -> Write Hub)
    logger.debug("TASK 3: Strategy (Deserializing State -> Final Signal)...")
    _ = json.loads(ohlc_json)
    _ = json.loads(sentiment_result)
    time.sleep(1.2)
    
    total = time.perf_counter() - start
    logger.success(f"Airflow-Sim Result: BUY BTC in {total:.2f}s")
    return {"total_time": total}

if __name__ == "__main__":
    run_airflow_sim_baseline()
