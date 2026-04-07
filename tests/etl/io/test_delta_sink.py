import pytest
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
    
    # We patch `deltalake.write_deltalake` used inside the implementation
    with patch("etl.io.sinks.delta_lake.write_deltalake") as mock_write:
        with patch("etl.io.sinks.delta_lake.pa.Table") as mock_table:
            # Mock the Table.from_pandas conversion
            mock_table.from_pandas.return_value = data
            
            with sink_config.open() as writer:
                writer.write_batch(data)
                
            # Verify write_deltalake was called
            mock_write.assert_called_once()


