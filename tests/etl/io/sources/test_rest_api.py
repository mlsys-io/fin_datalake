import pytest
from unittest.mock import MagicMock, patch
from etl.io.sources.rest_api import RestApiSource, RestApiReader, PaginationConfig

@pytest.fixture
def mock_requests():
    with patch("requests.Session") as mock:
        yield mock

def test_rest_api_source_config():
    source = RestApiSource(url="http://test.com")
    assert source.url == "http://test.com"
    assert source.method == "GET"

def test_reader_fetch_single_page(mock_requests):
    # Setup Mock
    session = mock_requests.return_value
    full_response = MagicMock()
    full_response.json.return_value = {"data": [{"id": 1}, {"id": 2}]}
    session.request.return_value = full_response
    
    # Test
    source = RestApiSource(url="http://test.com")
    with source.open() as reader:
        batches = list(reader.read_batch())
        
    assert len(batches) == 1
    assert len(batches[0]) == 2
    assert batches[0][0]["id"] == 1
    
    # Verify call
    session.request.assert_called_with(method="GET", url="http://test.com", params={})

def test_reader_pagination(mock_requests):
    # Setup Mock
    session = mock_requests.return_value
    
    # Page 1 response
    page1 = MagicMock()
    page1.json.return_value = [{"id": 1}]
    
    # Page 2 response (Empty to stop loop)
    # The reader checks "if not data" to break
    page2 = MagicMock()
    page2.json.return_value = [] 
    
    session.request.side_effect = [page1, page2]
    
    # Test
    source = RestApiSource(
        url="http://test.com",
        pagination=PaginationConfig(type="page", page_param="p")
    )
    
    with source.open() as reader:
        batches = list(reader.read_batch())
        
    assert len(batches) == 1 # Only page 1 had data
    
    # Verify calls
    assert session.request.call_count == 2
    # Check params of last call
    call_args_list = session.request.call_args_list
    assert call_args_list[0][1]['params']['p'] == 1
    assert call_args_list[1][1]['params']['p'] == 2

def test_reader_fetch_one_error_logging(mock_requests):
    """Verify Bug 3: _fetch_one uses logger correctly on error."""
    session = mock_requests.return_value
    session.request.side_effect = Exception("HTTP Error")
    
    source = RestApiSource(url="http://test.com")
    reader = RestApiReader(source)
    reader._session = session
    
    with pytest.raises(Exception) as excinfo:
        reader._fetch_one({})
    
    assert "HTTP Error" in str(excinfo.value)
    # If logger was missing, it would raise NameError instead of the original Exception.
