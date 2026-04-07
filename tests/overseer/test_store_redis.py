import pytest
import asyncio
from unittest.mock import AsyncMock, patch

from overseer.store import MetricsStore
from overseer.models import SystemSnapshot, OverseerAction, ActionType


@pytest.fixture
def mock_redis():
    from unittest.mock import MagicMock
    with patch("redis.asyncio.from_url") as mock_from_url:
        mock_client = AsyncMock()
        mock_pipeline = AsyncMock()
        
        # Setup context manager for pipeline properly
        mock_pipeline.__aenter__.return_value = mock_pipeline
        mock_pipeline.__aexit__.return_value = None
        
        # redis.pipeline() is a sync call returning an object, not a coroutine
        mock_client.pipeline = MagicMock(return_value=mock_pipeline)
        
        mock_from_url.return_value = mock_client
        yield mock_client, mock_pipeline


@pytest.mark.asyncio
async def test_store_latest(mock_redis):
    client, _ = mock_redis
    
    # Mock the return from Redis
    snapshot = SystemSnapshot()
    import json, dataclasses
    client.lindex.return_value = json.dumps(dataclasses.asdict(snapshot))
    
    store = MetricsStore()
    result = await store.latest()
    
    assert result is not None
    assert result.timestamp == snapshot.timestamp
    client.lindex.assert_called_once_with("overseer:snapshots", 0)


@pytest.mark.asyncio
async def test_store_history(mock_redis):
    client, _ = mock_redis
    
    snap1 = SystemSnapshot()
    snap2 = SystemSnapshot()
    
    import json, dataclasses
    # Redis lrange returns a list of strings
    client.lrange.return_value = [
        json.dumps(dataclasses.asdict(snap2)), 
        json.dumps(dataclasses.asdict(snap1))
    ]
    
    store = MetricsStore()
    history = await store.history(2)
    
    assert len(history) == 2
    # Ensure reverse chronological formatting matches UI expectations
    assert history[0]["timestamp"] == snap1.timestamp
    assert history[1]["timestamp"] == snap2.timestamp


@pytest.mark.asyncio
async def test_store_append_commands(mock_redis):
    _, pipeline = mock_redis
    
    store = MetricsStore(max_snapshots=10)
    await store.append_snapshot(SystemSnapshot())
    
    pipeline.lpush.assert_called_once()
    pipeline.ltrim.assert_called_once_with("overseer:snapshots", 0, 9)
    pipeline.execute.assert_called_once()
