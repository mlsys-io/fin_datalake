import pytest
from unittest.mock import MagicMock, patch
from etl.io.sources.rest_api import RestApiSource

def test_rest_source_config():
    """Test that config is stored correctly."""
    source = RestApiSource(
        url="https://api.test.com",
        method="POST",
        headers={"Auth": "Key"}
    )
    assert source.url == "https://api.test.com"
    assert source.method == "POST"
    assert source.headers["Auth"] == "Key"

def test_rest_source_read_batch():
    """Test runtime reading logic with mocked network."""
    source_config = RestApiSource(url="http://mock.api/data")
    
    # Mock Response Data
    mock_data = [{"id": 1}, {"id": 2}]
    
    with patch("requests.Session") as MockSession:
        # User Context Manager mock
        mock_instance = MockSession.return_value
        mock_response = MagicMock()
        mock_response.json.return_value = {"items": mock_data, "next": None}
        mock_response.raise_for_status.return_value = None
        mock_instance.request.return_value = mock_response
        
        # Open Reader
        with source_config.open() as reader:
            batches = list(reader.read_batch())
            
        # Assertions
        assert len(batches) == 1
        assert batches[0] == mock_data
        
        # Verify request call
        mock_instance.request.assert_called_with(
            method="GET",
            url="http://mock.api/data",
            params={}
        )

def test_rest_source_pagination():
    """Test that reader follows pagination links."""
    from etl.io.sources.rest_api import PaginationConfig
    
    source_config = RestApiSource(
        url="http://mock.api/page1",
        pagination=PaginationConfig(type="page", page_param="page")
    )
    
    with patch("requests.Session") as MockSession:
        mock_instance = MockSession.return_value
        
        # Response 1: Has next link
        resp1 = MagicMock()
        resp1.json.return_value = {"items": [{"p": 1}], "next": "http://mock.api/page2"}
        
        # Response 2: No next link
        resp2 = MagicMock()
        resp2.json.return_value = {"items": [{"p": 2}], "next": None}
        
        mock_instance.request.side_effect = [resp1, resp2]
        
        with source_config.open() as reader:
            batches = list(reader.read_batch())
            
        assert len(batches) == 2
        assert batches[0] == [{"p": 1}]
        assert batches[1] == [{"p": 2}]
