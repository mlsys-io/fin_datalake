"""
Tests for Overseer config loader: YAML parsing and env-var overrides.
"""

import pytest
from unittest.mock import patch
from pathlib import Path

from overseer.config import load_config, load_endpoints


CONFIG_DIR = Path(__file__).resolve().parents[2] / "app-code" / "overseer"
DEFAULT_CONFIG = CONFIG_DIR / "config.yaml"


class TestLoadConfig:
    def test_loads_default_config(self):
        raw = load_config(DEFAULT_CONFIG)
        assert "services" in raw
        assert "ray" in raw["services"]

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            load_config("/nonexistent/path/config.yaml")


class TestLoadEndpoints:
    def test_returns_list_of_endpoints(self):
        endpoints = load_endpoints(DEFAULT_CONFIG)
        assert len(endpoints) > 0
        names = [ep.name for ep in endpoints]
        assert "ray" in names
        assert "kafka" in names

    def test_ray_endpoint_defaults(self):
        endpoints = load_endpoints(DEFAULT_CONFIG)
        ray_ep = next(ep for ep in endpoints if ep.name == "ray")
        assert ray_ep.port == 8265
        assert ray_ep.protocol == "http"

    @patch.dict("os.environ", {"OVERSEER_RAY_HOST": "custom-host", "OVERSEER_RAY_PORT": "9999"})
    def test_env_var_override(self):
        endpoints = load_endpoints(DEFAULT_CONFIG)
        ray_ep = next(ep for ep in endpoints if ep.name == "ray")
        assert ray_ep.host == "custom-host"
        assert ray_ep.port == 9999

    def test_kafka_has_consumer_group_extra(self):
        endpoints = load_endpoints(DEFAULT_CONFIG)
        kafka_ep = next(ep for ep in endpoints if ep.name == "kafka")
        assert "consumer_group" in kafka_ep.extra
