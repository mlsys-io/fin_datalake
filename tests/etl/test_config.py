import pytest
import os
from unittest.mock import patch
from etl.config import Config, config

def test_config_defaults():
    """Verify Bug 10: String defaults are "" instead of None."""
    # Check some fields that were previously None
    assert isinstance(Config.HIVE_HOST, str)
    assert Config.HIVE_HOST == "" or os.environ.get("HIVE_HOST", "") == Config.HIVE_HOST
    
    assert isinstance(Config.TSDB_HOST, str)
    assert isinstance(Config.KAFKA_BOOTSTRAP_SERVERS, str)

def test_config_reload():
    """Verify Bug 9: Config.reload() refreshes all fields."""
    with patch.dict(os.environ, {"NODE_IP": "1.2.3.4", "TSDB_PORT": "9999"}):
        Config.reload()
        assert config.NODE_IP == "1.2.3.4"
        assert config.TSDB_PORT == 9999
        
    # Reset for other tests if necessary, although reload() should be idempotent
    Config.reload()
