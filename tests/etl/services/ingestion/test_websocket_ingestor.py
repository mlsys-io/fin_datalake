import pytest
from unittest.mock import MagicMock, patch
from etl.services.ingestion.websocket_ingestor import WebSocketIngestorService

def test_websocket_ingestor_init():
    """Verify Bug 8: WebSocketIngestorService uses standard __init__."""
    config = {
        "source": {"url": "ws://test"},
        "sink": {
            "host": "localhost",
            "user": "app",
            "password": "pwd",
            "database": "db",
            "table_name": "tbl"
        }
    }
    service = WebSocketIngestorService(name="ws_test", config=config)
    
    assert service.name == "ws_test"
    assert service.source_config == config["source"]
    assert service.sink_config == config["sink"]

def test_websocket_ingestor_run_logging():
    """Verify Bug 6: WebSocketIngestorService uses logger instead of update_status."""
    config = {
        "source": {"url": "ws://test"},
        "sink": {
            "host": "localhost",
            "user": "app",
            "password": "pwd",
            "database": "db",
            "table_name": "tbl"
        }
    }
    service = WebSocketIngestorService(name="ws_test", config=config)
    service.running = True
    
    with patch("etl.io.sources.websocket.WebSocketSource.open") as mock_src, \
         patch("etl.io.sinks.timescaledb.TimescaleDBSink.open") as mock_snk, \
         patch("loguru.logger.info") as mock_logger:
        
        mock_reader = MagicMock()
        # Return one batch then stop
        mock_reader.read_batch.return_value = [{"data": 1}]
        mock_src.return_value.__enter__.return_value = mock_reader
        
        mock_writer = MagicMock()
        mock_snk.return_value.__enter__.return_value = mock_writer
        
        # We need to ensure the loop breaks after one iteration for testing
        # Mocking read_batch to return a list of one batch
        service.run()
        
        # Verify logger was called (replaces update_status)
        mock_logger.assert_called()
        args, _ = mock_logger.call_args
        assert "Ingested 1 messages" in args[0]
