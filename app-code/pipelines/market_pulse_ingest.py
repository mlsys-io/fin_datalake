from typing import Dict, Any, List
import asyncio
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from loguru import logger
import os

from etl.io.sources.rest_api import RestApiSource
from etl.io.sources.websocket import WebSocketSource
from etl.io.sinks.delta_lake import DeltaLakeSink

# Ensure FINNHUB_API_KEY is available in the environment
FINNHUB_KEY = os.environ.get("FINNHUB_API_KEY", "sandbox_key")

@task
def ingest_news():
    """Extracts financial headlines via REST API and writes to Delta Lake."""
    logger.info("[Ingest] Starting News ingestion via REST...")
    
    # Configure source for Finnhub's general market news
    source = RestApiSource(
        base_url="https://finnhub.io/api/v1",
        endpoint="/news",
        headers={"X-Finnhub-Token": FINNHUB_KEY},
        params={"category": "general"},
        auth=None
    )
    
    # Configure sink to Delta Lake Bronze layer
    sink = DeltaLakeSink(
        uri="s3://lakehouse/demo/bronze/news", 
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        mode="overwrite" # For demo purposes, we overwrite to stay clean
    )
    
    # Read the data (creates a Ray dataset internally or pandas chunk)
    with source.open() as reader:
        # fetch_data returns an iterator of batches
        batches = list(reader.fetch_data())
        logger.info(f"[Ingest] Retrieved {len(batches)} batches of news from Finnhub.")
        
        # In a real scenario we'd use a pipeline; here we just write the first batch directly
        if batches and len(batches) > 0:
             # Normalize data to ensure Schema compliance
             import pandas as pd
             df = pd.DataFrame(batches[0])
             # Keep only relevant columns
             if not df.empty:
                 cols_to_keep = ["headline", "source", "url", "datetime"]
                 df = df[[c for c in cols_to_keep if c in df.columns]]
                 
                 with sink.open() as writer:
                     writer.write_batch(df)
                     logger.success("[Ingest] News written to Delta Lake!")
                 return df.to_dict(orient="records")
    return []

@task
def ingest_ohlc():
    """Extracts live tick data via WebSocket and writes to Delta Lake."""
    logger.info("[Ingest] Starting OHLC streaming via WebSocket...")
    
    # Normally we would use 'wss://ws.finnhub.io?token=' but the generic 
    # WebSocketSource needs a clean URI. 
    source = WebSocketSource(
        uri=f"wss://ws.finnhub.io?token={FINNHUB_KEY}",
        payload={"type": "subscribe", "symbol": "BINANCE:BTCUSDT"},
        max_messages=10, # Capture 10 ticks for the demo
        timeout=15.0
    )
    
    sink = DeltaLakeSink(
        uri="s3://lakehouse/demo/bronze/ohlc",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        mode="append"
    )
    
    # Read data
    with source.open() as reader:
        # fetch_data is typically a generator of batches
        try:
             # Since it's a websocket, we consume it synchronously for this chunk
             # In a real streaming architecture this would be a continuous process
             batches = list(reader.fetch_data())
             logger.info(f"[Ingest] Captured {len(batches)} ticks from WebSocket.")
             
             if batches and len(batches) > 0:
                 import pandas as pd
                 # WS source yields raw JSON messages. Finnhub returns 'data' arrays.
                 all_ticks = []
                 for batch in batches:
                     # Parse finnhub structure: {"data": [{"p": 65000, "v": 0.1, ...}], "type": "trade"}
                     if isinstance(batch, list):
                         for msg in batch:
                            if isinstance(msg, dict) and msg.get("type") == "trade":
                                for trade in msg.get("data", []):
                                    all_ticks.append({
                                        "symbol": "BTCUSD",
                                        "close": trade.get("p"),
                                        "volume": trade.get("v"),
                                        "timestamp": trade.get("t")
                                    })
                 
                 if all_ticks:
                     df = pd.DataFrame(all_ticks)
                     with sink.open() as writer:
                         writer.write_batch(df)
                         logger.success("[Ingest] OHLC ticks written to Delta Lake!")
                     return df.to_dict(orient="records")
        except Exception as e:
             logger.error(f"[Ingest] Websocket capture failed: {e}")
             
    # If the WS fails (e.g. rate limit), return fallback data
    logger.warning("[Ingest] Using fallback synthetic OHLC data.")
    return [{"symbol": "BTCUSD", "close": 65000 + i*10, "volume": 1.5, "timestamp": 1700000000 + i} for i in range(20)]


@flow(
    name="Market Pulse Ingest",
    description="Fetches News and Trades to Delta Lake in parallel",
    task_runner=ConcurrentTaskRunner(),
)
def market_pulse_ingest() -> Dict[str, List[Any]]:
    """
    Master ingestion flow.

    Submits the REST news task and the WebSocket OHLC task concurrently
    using Prefect's ConcurrentTaskRunner (thread-based parallelism).
    Both tasks write independently to Delta Lake; results are collected
    once both complete.
    """
    logger.info("=== Starting Market Pulse Ingest Flow (parallel) ===")

    # Submit both tasks concurrently — they return PrefectFutures immediately
    news_future = ingest_news.submit()
    ohlc_future = ingest_ohlc.submit()

    # Block until both are done and retrieve results
    news_data = news_future.result()
    ohlc_data = ohlc_future.result()

    logger.info(f"=== Ingestion Complete! ===")
    logger.info(f"News records: {len(news_data)}")
    logger.info(f"OHLC records: {len(ohlc_data)}")

    return {
        "news": news_data,
        "ohlc": ohlc_data
    }

if __name__ == "__main__":
    market_pulse_ingest()
