import pytest
import pandas as pd
import pyarrow as pa
from unittest.mock import MagicMock, patch
from etl.io.sinks.delta_lake import DeltaLakeSink, DeltaLakeWriter

@pytest.fixture
def mock_write_deltalake():
    with patch("deltalake.write_deltalake") as mock:
        yield mock

def test_sink_config():
    sink = DeltaLakeSink(uri="s3://bucket/table")
    assert sink.mode == "append"

def test_writer_clean_schema():
    # Create DF with timezone
    df = pd.DataFrame({'time': pd.to_datetime(['2024-01-01']).tz_localize('UTC')})
    pa_table = pa.Table.from_pandas(df)
    
    sink = DeltaLakeSink(uri="tmp")
    writer = DeltaLakeWriter(sink)
    
    cleaned = writer._clean_schema(pa_table)
    
    # Check that TZ is gone
    field = cleaned.schema.field('time')
    assert field.type.tz is None

def test_write_batch_calls_deltalake(mock_write_deltalake):
    df = pd.DataFrame({"col": [1, 2]})
    sink = DeltaLakeSink(uri="s3://test", aws_access_key_id="key", aws_secret_access_key="secret")
    
    with sink.open() as writer:
        writer.write_batch(df)
        
    mock_write_deltalake.assert_called_once()
    call_kwargs = mock_write_deltalake.call_args[1]
    assert call_kwargs["table_or_uri"] == "s3://test"
    assert call_kwargs["storage_options"]["AWS_ACCESS_KEY_ID"] == "key"

def test_foolproof_hive_registration(mock_write_deltalake):
    # Mock Hive
    with patch("etl.services.hive.HiveMetastore") as mock_hms_class:
        mock_hms = mock_hms_class.return_value
        mock_client = MagicMock()
        mock_hms.open.return_value.__enter__.return_value = mock_client
        
        sink = DeltaLakeSink(
            uri="s3://test",
            hive_config={"host": "localhost", "port": 9083},
            hive_table_name="db.table"
        )
        
        df = pd.DataFrame({"col": [1]})
        
        with sink.open() as writer:
            writer.write_batch(df)
            
        # Verify Hive was called
        mock_client.register_delta_table.assert_called_once()
        assert mock_client.register_delta_table.call_args[1]["table_name"] == "table"

def test_writer_filters_none_storage_options():
    """Verify Bug 5: None values are removed from storage_options."""
    sink = DeltaLakeSink(uri="s3://test", aws_access_key_id=None)
    writer = DeltaLakeWriter(sink)
    opts = writer._storage_options
    assert "AWS_ACCESS_KEY_ID" not in opts
    # Check that other keys are still there
    assert "AWS_REGION" in opts
