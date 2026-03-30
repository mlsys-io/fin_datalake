import pytest
import ray
import pandas as pd
import pyarrow as pa
from unittest.mock import MagicMock, patch

from etl.agents.sentiment_agent import SentimentAgent
from sample_agents.strategy_agent import StrategyAgent
from etl.agents.hub import get_agent_hub
from etl.agents.context import get_context_store

@pytest.fixture(scope="module")
def ray_instance():
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)
    yield
    # ray.shutdown() # Keep running for speed if multiple tests

def test_sentiment_agent_heuristic(ray_instance):
    """Verify sentiment agent keyword fallback works without an API key."""
    SentimentAgent.deploy(name="TestSentiment")
    hub = get_agent_hub()
    handle = hub.get_agent_raw("TestSentiment")
    
    payload = ["Bitcoin surges to new highs", "Market crashes amid fears"]
    results = ray.get(handle.ask.remote(payload))
    
    assert len(results) == 2
    assert results[0]["sentiment"] == "bullish"
    assert results[1]["sentiment"] == "bearish"
    assert results[0]["score"] > 0
    assert results[1]["score"] < 0

def test_strategy_agent_delegation(ray_instance):
    """Verify StrategyAgent can delegate to SentimentAgent and produce a signal."""
    # Ensure both are deployed
    SentimentAgent.deploy(name="MarketSentiment-1")
    StrategyAgent.deploy(name="CryptoStrategy-1")
    
    hub = get_agent_hub()
    strategy_handle = hub.get_agent("strategy")
    
    # Mock data
    ohlc = [{"close": 60000 + i, "volume": 1.0} for i in range(25)] # SMA5 > SMA20
    headlines = ["Bullish news here"]
    
    payload = {
        "symbol": "BTCUSD",
        "ohlc_data": ohlc,
        "headlines": headlines
    }
    
    result = ray.get(strategy_handle.ask.remote(payload))
    
    assert "action" in result
    assert result["symbol"] == "BTCUSD"
    assert result["action"] in ["BUY", "SELL", "HOLD"]
    assert "confidence" in result
    
    # Verify ContextStore publication
    ctx = get_context_store()
    saved = ctx.get("signal:BTCUSD")
    assert saved is not None
    assert saved["action"] == result["action"]

def test_zero_copy_resolution(ray_instance):
    """Verify StrategyAgent can resolve a Ray ObjectRef (Zero-Copy path)."""
    StrategyAgent.deploy(name="ZCAgent")
    hub = get_agent_hub()
    handle = hub.get_agent_raw("ZCAgent")
    
    # Create Arrow table and put in object store
    df = pd.DataFrame({"close": [60000]*25, "volume": [1.0]*25})
    table = pa.Table.from_pandas(df)
    ref = ray.put(table)
    
    payload = {
        "symbol": "BTCUSD",
        "data_ref": ref,
        "headlines": []
    }
    
    # This triggers self._resolve_data() logic
    result = ray.get(handle.ask.remote(payload))
    assert result["symbol"] == "BTCUSD"
    assert "action" in result
