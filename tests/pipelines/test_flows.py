import pytest
from unittest.mock import MagicMock, patch

from pipelines.demo_pipeline import fetch_data, filter_data, transform_data


def test_fetch_task():
    response = MagicMock()
    response.raise_for_status.return_value = None
    response.json.return_value = [{"id": 1, "title": "alpha"}, {"id": 10, "title": "beta"}]

    with patch("requests.get", return_value=response) as mock_get:
        result = fetch_data.fn(url="http://test.com")

    assert result == [{"id": 1, "title": "alpha"}, {"id": 10, "title": "beta"}]
    mock_get.assert_called_once_with("http://test.com", timeout=30)


def test_filter_and_transform_tasks():
    data = [
        {"id": 1, "title": "skip"},
        {"id": 10, "title": "keep"},
    ]

    filtered = filter_data.fn(data, min_id=5)
    transformed = transform_data.fn(filtered)

    assert len(filtered) == 1
    assert filtered[0]["id"] == 10
    assert len(transformed) == 1
    assert transformed[0]["title_length"] == 4
