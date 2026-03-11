"""Unit tests for HttpSink."""
import pytest
from unittest.mock import MagicMock, patch
import pandas as pd


def test_http_sink_config():
    """Test that config is stored correctly."""
    from etl.io.sinks.http import HttpSink
    
    sink = HttpSink(
        url="https://api.example.com/webhook",
        auth_token="secret-token",
        batch_key="events"
    )
    
    assert sink.url == "https://api.example.com/webhook"
    assert sink.method == "POST"
    assert sink.auth_token == "secret-token"
    assert sink.batch_key == "events"
    assert sink.auth_type == "bearer"


def test_http_sink_validation():
    """Test that invalid config raises errors."""
    from etl.io.sinks.http import HttpSink
    
    # Invalid URL should fail
    with pytest.raises(ValueError, match="Invalid URL"):
        HttpSink(url="not-a-valid-url")
    
    # Invalid auth_type should fail
    with pytest.raises(ValueError, match="auth_type"):
        HttpSink(
            url="https://example.com",
            auth_type="invalid"
        )


def test_http_writer_write_batch():
    """Test batch writing with mocked HTTP session."""
    from etl.io.sinks.http import HttpSink
    
    sink = HttpSink(
        url="https://api.example.com/webhook",
        batch_key="data"
    )
    
    with patch("requests.Session") as MockSession:
        mock_session = MockSession.return_value
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_session.request.return_value = mock_response
        
        with sink.open() as writer:
            writer.write_batch([{"id": 1}, {"id": 2}])
        
        # Verify request was made
        mock_session.request.assert_called_once()
        call_args = mock_session.request.call_args
        assert call_args.kwargs["method"] == "POST"
        assert call_args.kwargs["url"] == "https://api.example.com/webhook"
        assert call_args.kwargs["json"] == {"data": [{"id": 1}, {"id": 2}]}


def test_http_writer_dataframe():
    """Test writing DataFrame data."""
    from etl.io.sinks.http import HttpSink
    
    sink = HttpSink(url="https://example.com/api", batch_key="records")
    
    df = pd.DataFrame([{"col": 1}, {"col": 2}])
    
    with patch("requests.Session") as MockSession:
        mock_session = MockSession.return_value
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_session.request.return_value = mock_response
        
        with sink.open() as writer:
            writer.write_batch(df)
        
        call_args = mock_session.request.call_args
        payload = call_args.kwargs["json"]
        assert len(payload["records"]) == 2
