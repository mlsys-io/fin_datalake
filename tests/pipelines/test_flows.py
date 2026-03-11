import pytest
from unittest.mock import MagicMock, patch
from app_code.demo_pipeline import main_pipeline, ingest_data

# Ensure we can import modules
import sys
import os
sys.path.append(os.path.join(os.getcwd(), "app-code"))

@pytest.fixture
def mock_ingestion():
    with patch("etl.ingestion.rest_api_ingestion.RestApiIngestion") as mock:
        instance = mock.return_value
        instance.__enter__.return_value = instance
        # Simulate two batches of data
        instance.fetch_data.return_value = [[{"id": 1}], [{"id": 2}]]
        yield mock

def test_ingest_task(mock_ingestion):
    """Test individual task logic"""
    result = ingest_data.fn(url="http://test.com") # .fn calls the raw python function
    assert len(result) == 2
    assert result[0]["id"] == 1

def test_pipeline_execution():
    """Test the full flow execution (locally)"""
    # We strip the task runner for unit tests to run in-process simply
    # or just run main_pipeline directly which defaults to running locally
    # if Ray isn't active/configured.
    
    with patch("etl.ingestion.rest_api_ingestion.RestApiIngestion") as mock_conn:
        instance = mock_conn.return_value
        instance.__enter__.return_value = instance
        instance.fetch_data.return_value = [[{"id": 10}, {"id": 20}]]
        
        result = main_pipeline(api_url="http://test.com")
        
        assert len(result) == 2
        assert result[0]["id"] == 10
