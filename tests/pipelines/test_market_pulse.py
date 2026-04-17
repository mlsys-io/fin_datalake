import pytest
import ray
import pandas as pd
import pyarrow as pa
from unittest.mock import patch

from etl.agents.sentiment_agent import SentimentAgent
from sample_agents.strategy_agent import StrategyAgent


@pytest.fixture(scope="module")
def ray_instance():
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)
    yield


def test_sentiment_agent_heuristic():
    """Verify sentiment agent keyword fallback works without an API key."""
    agent = SentimentAgent()

    payload = ["Bitcoin surges to new highs", "Market crash amid fears"]
    results = agent.ask(payload)

    assert len(results) == 2
    assert results[0]["sentiment"] == "bullish"
    assert results[1]["sentiment"] == "bearish"
    assert results[0]["score"] > 0
    assert results[1]["score"] < 0


def test_strategy_agent_delegation():
    """Verify StrategyAgent can combine inputs into a trading signal."""
    agent = StrategyAgent()

    ohlc = [{"close": 60000 + i, "volume": 1.0} for i in range(25)]
    headlines = ["Bullish news here"]

    payload = {
        "symbol": "BTCUSD",
        "ohlc_data": ohlc,
        "headlines": headlines,
    }

    with patch.object(
        StrategyAgent,
        "delegate",
        return_value={
            "label": "bullish",
            "score": 0.8,
            "summary": "Bullish news",
            "headlines": headlines,
        },
    ):
        result = agent.ask(payload)

    assert "action" in result
    assert result["symbol"] == "BTCUSD"
    assert result["action"] in ["BUY", "SELL", "HOLD"]
    assert "confidence" in result


def test_zero_copy_resolution(ray_instance):
    """Verify StrategyAgent can resolve a Ray ObjectRef (Zero-Copy path)."""
    agent = StrategyAgent()

    df = pd.DataFrame({"close": [60000] * 25, "volume": [1.0] * 25})
    table = pa.Table.from_pandas(df)
    ref = ray.put(table)

    payload = {
        "symbol": "BTCUSD",
        "data_ref": ref,
        "headlines": [],
    }

    result = agent.ask(payload)
    assert result["symbol"] == "BTCUSD"
    assert "action" in result
