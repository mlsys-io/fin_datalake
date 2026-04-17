import pytest
import sys
import types
import pandas as pd
from unittest.mock import MagicMock, patch
from etl.io.sinks.delta_lake import DeltaLakeSink

def test_delta_sink_config():
    """Test config storage."""
    sink = DeltaLakeSink(uri="s3://bucket/table", mode="overwrite")
    assert sink.uri == "s3://bucket/table"
    assert sink.mode == "overwrite"

def test_delta_sink_write_batch():
    """Test writing logic mocking the heavy deltalake lib."""
    sink_config = DeltaLakeSink(uri="/tmp/delta", mode="append")
    
    data = pd.DataFrame([{"col": 1}, {"col": 2}])
    
    fake_deltalake = types.ModuleType("deltalake")
    fake_deltalake.write_deltalake = MagicMock()

    with patch.dict(sys.modules, {"deltalake": fake_deltalake}):
        with sink_config.open() as writer:
            writer.write_batch(data)

    # Verify write_deltalake was called
    fake_deltalake.write_deltalake.assert_called_once()


