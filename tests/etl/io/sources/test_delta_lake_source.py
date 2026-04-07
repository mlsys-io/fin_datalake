import pytest
from unittest.mock import MagicMock, patch
from etl.io.sources.delta_lake import DeltaLakeSource, DeltaLakeReader

def test_reader_filters_none_storage_options():
    """Verify Bug 5: None values are removed from storage_options in DeltaLakeReader."""
    source = DeltaLakeSource(uri="s3://test", aws_access_key_id=None)
    reader = DeltaLakeReader(source)
    opts = reader._storage_options
    assert "AWS_ACCESS_KEY_ID" not in opts
    # Check that other keys are still there
    assert "AWS_REGION" in opts
