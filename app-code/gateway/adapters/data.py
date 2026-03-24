"""
DataAdapter: Domain "data"

Handles read-oriented operations against the Lakehouse storage layer.

Supported Actions:
    - run_sql:      Execute a DuckDB SQL query against Delta Lake.
    - get_schema:   Return the schema of a specific Delta table.
    - list_tables:  List available Delta tables in the Lakehouse.
    - preview:      Return the first N rows of a table as JSON.

Required Permission: data:read for all actions.
"""

from typing import Any

from gateway.core.adapters import BaseAdapter, ActionNotFoundError
from gateway.core.rbac import Permission
from gateway.models.intent import UserIntent
from gateway.models.user import User

import os
import json
import asyncio
import concurrent.futures
from loguru import logger

# Single-threaded executor serializes all DuckDB calls — avoids thread-safety issues
_duckdb_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)


class DataAdapter(BaseAdapter):

    def handles(self) -> str:
        return "data"

    @staticmethod
    def _validate_table_path(path: str):
        """
        Validate the table path to prevent SQL injection.
        Allows alphanumeric, forward slashes, colons, underscores, hyphens, and periods.
        """
        import re
        if not path:
            raise ValueError("Table path cannot be empty.")
        if len(path) > 256:
            raise ValueError("Table path is too long (max 256 chars).")
        # Allow s3://, local paths, and basic identifiers
        if not re.match(r"^[a-zA-Z0-9_\-\.\/\:]+$", path):
            raise ValueError(f"Invalid table path: '{path}'. Contains prohibited characters.")

    async def execute(self, user: User, intent: UserIntent) -> Any:
        self._require_permission(user, Permission.DATA_READ)

        dispatch = {
            "run_sql": self._run_sql,
            "get_schema": self._get_schema,
            "list_tables": self._list_tables,
            "preview": self._preview,
        }
        handler = dispatch.get(intent.action)
        if handler is None:
            raise ActionNotFoundError(
                f"DataAdapter does not support action '{intent.action}'. "
                f"Available: {list(dispatch.keys())}"
            )
        return await handler(intent)

    async def _run_sql(self, intent: UserIntent) -> dict:
        """Execute a SQL query against Delta Lake via DuckDB.
        
        Uses a dedicated single-threaded executor to serialize all DuckDB calls,
        which avoids thread-safety issues with concurrent requests.
        """
        sql = intent.parameters.get("sql")
        if not sql:
            raise ValueError("Parameter 'sql' is required.")
        
        def _execute_duckdb():
            import duckdb
            conn = duckdb.connect()  # in-memory, cheap and safe per-call
            return conn.execute(sql).fetchdf()

        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(_duckdb_executor, _execute_duckdb)
            
            # Elite Feature: Zero-Copy Fast-Path
            # If requested, put the dataframe into Ray Plasma Store and return the ID.
            prefer_ref = intent.parameters.get("prefer_ref", False)
            if prefer_ref:
                try:
                    import ray
                    if ray.is_initialized():
                        ref = ray.put(result)
                        return {
                            "success": True,
                            "mode": "zero_copy",
                            "object_ref_id": ref.hex(),
                            "row_count": len(result),
                            "columns": list(result.columns)
                        }
                except (ImportError, Exception) as re:
                     logger.warning(f"Zero-copy failed, falling back to JSON: {re}")

            return {
                "success": True,
                "mode": "json",
                "columns": list(result.columns),
                "rows": result.values.tolist(),
                "row_count": len(result),
            }
        except Exception as e:
            return {
                "success": False,
                "error_type": type(e).__name__,
                "error_message": str(e),
            }

    async def _get_schema(self, intent: UserIntent) -> dict:
        """Return the schema fields of a Delta table."""
        from deltalake import DeltaTable
        table_path = intent.parameters.get("table_path")
        if not table_path:
            raise ValueError("Parameter 'table_path' is required.")
        self._validate_table_path(table_path)
        
        def _fetch_schema():
            dt = DeltaTable(table_path)
            return dt.schema()

        loop = asyncio.get_event_loop()
        schema = await loop.run_in_executor(None, _fetch_schema)
        
        return {"table_path": table_path, "fields": [
            {"name": f.name, "type": str(f.type)} for f in schema.fields
        ]}

    async def _list_tables(self, intent: UserIntent) -> dict:
        """List available Delta tables from Hive Metastore with Redis caching."""
        from redis.asyncio import Redis
        
        # 1. Try Cache First
        from gateway.core.redis import get_redis_client
        r = get_redis_client()
        if not r:
             logger.warning("Redis not configured. Caching disabled.")
        
        cache_key = "gateway:cache:tables"
        try:
            async with r:
                cached = await r.get(cache_key)
                if cached:
                    return {"tables": json.loads(cached), "source": "cache"}
        except Exception as e:
            logger.warning(f"Redis cache lookup failed: {e}")

        # 2. Try Hive Metastore (in executor to avoid blocking loop)
        try:
            from etl.services.hive import HiveMetastore
            
            hms_host = os.environ.get("HMS_HOST", "localhost")
            hms_port = int(os.environ.get("HMS_PORT", 9083))
            
            def fetch_from_hive():
                config = HiveMetastore(host=hms_host, port=hms_port)
                with config.open() as client:
                    return client.get_all_tables(db="default")
            
            loop = asyncio.get_event_loop()
            tables = await loop.run_in_executor(None, fetch_from_hive)
            
            # Cache the result for 60s
            try:
                async with r:
                    await r.setex(cache_key, 60, json.dumps(tables))
            except Exception:
                pass
                
            return {"tables": tables, "source": "hive"}
            
        except Exception as e:
            logger.error(f"Hive Metastore lookup failed: {e}. Falling back to static list.")
            # 3. Fallback to static list
            return {
                "tables": [
                    {"name": "market_data", "path": "s3://delta-lake/bronze/market_data"},
                    {"name": "news_sentiment", "path": "s3://delta-lake/bronze/news"},
                ],
                "source": "fallback",
                "error": str(e)
            }

    async def _preview(self, intent: UserIntent) -> dict:
        """Return first N rows of a Delta table."""
        table_path = intent.parameters.get("table_path")
        limit = intent.parameters.get("limit", 10)
        if not table_path:
            raise ValueError("Parameter 'table_path' is required.")
        self._validate_table_path(table_path)
        preview_intent = UserIntent(
            domain="data", action="run_sql",
            parameters={"sql": f"SELECT * FROM delta_scan('{table_path}') LIMIT {limit}"},
            user_id=intent.user_id, roles=intent.roles,
        )
        return await self._run_sql(preview_intent)
