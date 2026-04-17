"""
Tests for the DataAdapter and BrokerAdapter.

Focuses on action routing, output format, and permission enforcement.
External backends (DuckDB, Prefect, Ray) are NOT called - only the
adapter's own dispatch logic is exercised.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from gateway.adapters.data import DataAdapter
from gateway.adapters.broker import BrokerAdapter
from gateway.core.adapters import ActionNotFoundError, PermissionError, AdapterExecutionError
from gateway.models.intent import UserIntent
from gateway.models.user import User


def make_intent(domain: str, action: str, **params) -> UserIntent:
    return UserIntent(
        domain=domain,
        action=action,
        parameters=params,
        user_id="test",
        roles=["admin"],
    )


ADMIN = User(username="admin", hashed_password="x", role_names=["Admin"])
ANALYST = User(username="analyst", hashed_password="x", role_names=["Analyst"])


class TestDataAdapter:
    def setup_method(self):
        self.adapter = DataAdapter()

    def test_handles_returns_data(self):
        assert self.adapter.handles() == "data"

    @pytest.mark.asyncio
    async def test_list_tables_returns_dict_with_tables_key(self):
        intent = make_intent("data", "list_tables")

        mock_redis_inst = MagicMock()
        mock_redis_inst.get = AsyncMock(return_value=None)
        mock_redis_inst.setex = AsyncMock()
        mock_redis_inst.__aenter__ = AsyncMock(return_value=mock_redis_inst)
        mock_redis_inst.__aexit__ = AsyncMock(return_value=None)

        with patch("gateway.core.redis.get_redis_client", return_value=mock_redis_inst), \
             patch("etl.integrations.hive.HiveMetastore") as mock_hms_class:
            mock_hms_inst = mock_hms_class.return_value
            mock_client = mock_hms_inst.open.return_value.__enter__.return_value
            mock_client.get_all_tables.return_value = [{"name": "m", "path": "s3://p"}]

            result = await self.adapter.execute(ANALYST, intent)
            assert "tables" in result
            assert isinstance(result["tables"], list)
            assert result.get("source") == "hive"

    @pytest.mark.asyncio
    async def test_list_tables_entries_have_name_and_path(self):
        intent = make_intent("data", "list_tables")

        mock_redis_inst = MagicMock()
        mock_redis_inst.get = AsyncMock(return_value=None)
        mock_redis_inst.setex = AsyncMock()
        mock_redis_inst.__aenter__ = AsyncMock(return_value=mock_redis_inst)
        mock_redis_inst.__aexit__ = AsyncMock(return_value=None)

        with patch("gateway.core.redis.get_redis_client", return_value=mock_redis_inst), \
             patch("etl.integrations.hive.HiveMetastore") as mock_hms_class:
            mock_hms_inst = mock_hms_class.return_value
            mock_client = mock_hms_inst.open.return_value.__enter__.return_value
            mock_client.get_all_tables.return_value = [{"name": "m", "path": "s3://p"}]

            result = await self.adapter.execute(ANALYST, intent)
            for table in result["tables"]:
                assert "name" in table
                assert "path" in table

    @pytest.mark.asyncio
    async def test_unknown_action_raises(self):
        intent = make_intent("data", "nonexistent")
        with pytest.raises(ActionNotFoundError):
            await self.adapter.execute(ADMIN, intent)

    @pytest.mark.asyncio
    async def test_run_sql_gracefully_handles_bad_sql(self):
        intent = make_intent("data", "run_sql", sql="SELECT * FROM table_that_does_not_exist")
        with patch("duckdb.connect") as mock_conn:
            import duckdb

            mock_conn.return_value.execute.side_effect = duckdb.CatalogException("table_that_does_not_exist")
            with pytest.raises(AdapterExecutionError) as exc:
                await self.adapter.execute(ANALYST, intent)

        assert exc.value.error_type == "CatalogException"
        assert "table_that_does_not_exist" in str(exc.value)

    @pytest.mark.asyncio
    async def test_preview_sql_injection_protection(self):
        """Assert that malicious table_path raises ValueError."""
        malicious_path = "'); DROP TABLE users; --"
        intent = make_intent("data", "preview", table_path=malicious_path)
        with pytest.raises(ValueError) as exc:
            await self.adapter.execute(ANALYST, intent)
        assert "prohibited characters" in str(exc.value)


class TestBrokerAdapter:
    def setup_method(self):
        self.adapter = BrokerAdapter()

    def test_handles_returns_broker(self):
        assert self.adapter.handles() == "broker"

    @pytest.mark.asyncio
    async def test_list_connections_allowed_for_admin(self):
        intent = make_intent("broker", "list_connections")
        result = await self.adapter.execute(ADMIN, intent)
        assert "available_connections" in result

    @pytest.mark.asyncio
    async def test_list_connections_has_minio_and_timescale(self):
        intent = make_intent("broker", "list_connections")
        result = await self.adapter.execute(ADMIN, intent)
        names = [c["name"] for c in result["available_connections"]]
        assert "minio" in names
        assert "timescaledb" in names

    @pytest.mark.asyncio
    @patch.dict(
        "os.environ",
        {
            "MINIO_ENDPOINT": "http://localhost:9000",
            "AWS_ACCESS_KEY_ID": "test-key",
            "AWS_SECRET_ACCESS_KEY": "test-secret",
        },
    )
    async def test_get_s3_creds_returns_credentials(self):
        intent = make_intent("broker", "get_s3_creds")
        result = await self.adapter.execute(ADMIN, intent)
        assert result["endpoint_url"] == "http://localhost:9000"
        assert result["access_key_id"] == "test-key"
        assert result["secret_access_key"] == "test-secret"

    @pytest.mark.asyncio
    async def test_get_s3_creds_denied_for_analyst(self):
        """Analyst has broker:read but NOT broker:vend."""
        intent = make_intent("broker", "get_s3_creds")
        with pytest.raises(PermissionError):
            await self.adapter.execute(ANALYST, intent)

    @pytest.mark.asyncio
    async def test_unknown_action_raises(self):
        intent = make_intent("broker", "fake_action")
        with pytest.raises(ActionNotFoundError):
            await self.adapter.execute(ADMIN, intent)
