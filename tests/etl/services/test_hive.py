import pytest
import pyarrow as pa
from unittest.mock import MagicMock, patch
from etl.services.hive import HiveMetastore, HiveClient

# We mock the dynamic imports in the module
@patch("etl.services.hive.TSocket")
@patch("etl.services.hive.TTransport")
@patch("etl.services.hive.TBinaryProtocol")
@patch("etl.services.hive.HMSvc")
def test_hive_connection(mock_svc, mock_proto, mock_trans, mock_sock):
    
    # Setup
    config = HiveMetastore(host="test", port=1000)
    
    # Action
    with config.open() as client:
        pass
        
    # Verify
    mock_sock.TSocket.assert_called_with("test", 1000)
    mock_trans.TBufferedTransport.assert_called()
    mock_svc.Client.assert_called()
    
    # Check usage of transport
    transport = mock_trans.TBufferedTransport.return_value
    transport.open.assert_called()
    transport.close.assert_called()

def test_arrow_to_hive_conversion():
    # Only testing the helper method, which doesn't need network
    config = HiveMetastore()
    client = HiveClient(config)
    
    schema = pa.schema([
        ("id", pa.int64()),
        ("name", pa.string()),
        ("is_valid", pa.bool_())
    ])
    
    cols = client._arrow_to_hive_columns(schema)
    
    assert cols[0] == ("id", "bigint")
    assert cols[1] == ("name", "string")
    assert cols[2] == ("is_valid", "boolean")
