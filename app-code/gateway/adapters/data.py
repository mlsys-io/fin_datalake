"""
DataAdapter: Domain "data"

Handles read-oriented operations against the Lakehouse storage layer.

Supported Actions:
    - run_sql:      Execute a DuckDB SQL query against Delta Lake.
    - query_stream: Execute a SQL query against RisingWave.
    - get_schema:   Return the schema of a specific Delta table.
    - list_tables:  List available Delta tables in the Lakehouse.
    - preview:      Return the first N rows of a table as JSON.
    - catalog_sources: Return grouped catalog sources across live and static storage families.

Required Permission: data:read for all actions.
"""

from datetime import datetime, timezone
from typing import Any

from gateway.core.adapters import BaseAdapter, ActionNotFoundError, AdapterExecutionError
from gateway.core.rbac import Permission
from gateway.models.intent import UserIntent
from gateway.models.user import User

import os
import json
import asyncio
import concurrent.futures
import re
from loguru import logger

# Single-threaded executor serializes all DuckDB calls — avoids thread-safety issues
_duckdb_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
_streamdb_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)


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

    @staticmethod
    def _storage_family_from_table(table: dict[str, Any], default: str = "Other") -> str:
        if not isinstance(table, dict):
            return default

        family = str(table.get("family") or table.get("storage_family") or table.get("source_family") or "").strip()
        if family:
            return family

        source = str(table.get("source") or table.get("source_type") or "").strip().lower()
        path = str(table.get("path") or table.get("table_path") or table.get("location") or "").strip().lower()
        name = str(table.get("name") or table.get("table_name") or table.get("table") or "").strip().lower()

        if source in {"risingwave", "streaming_sql"} or "risingwave" in path or "market_pulse" in name:
            return "streaming_sql"
        if source in {"timescaledb", "postgres", "pgsql"} or "timescale" in path or "postgres" in path or "pgsql" in path:
            return "postgres"
        if source in {"hive", "cache", "fallback", "lakehouse"} or path.startswith("s3://") or "delta" in path or "lake" in path:
            return "lakehouse"
        if "minio" in path or "object" in path:
            return "object_storage"
        if "sqlite" in path or "duckdb" in path or "local" in path:
            return "local_file"
        return default

    @staticmethod
    def _strip_sql_comments(sql: str) -> str:
        sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
        stripped_lines = []
        for line in sql.splitlines():
            stripped_lines.append(line.split("--", 1)[0])
        return "\n".join(stripped_lines)

    @classmethod
    def _validate_read_only_sql(cls, sql: str) -> str:
        if not sql or not str(sql).strip():
            raise ValueError("Parameter 'sql' is required.")

        cleaned = cls._strip_sql_comments(str(sql)).strip()
        if not cleaned:
            raise ValueError("SQL query cannot be empty.")

        trailing = cleaned.rstrip(";").strip()
        if ";" in trailing:
            raise ValueError("Multiple SQL statements are not allowed.")

        first_token_match = re.match(r"^[A-Za-z]+", trailing)
        if not first_token_match:
            raise ValueError("SQL query must start with a read-only statement.")

        first_keyword = first_token_match.group(0).lower()
        allowed_keywords = {"select", "with", "show", "describe", "explain"}
        forbidden_keywords = {
            "insert", "update", "delete", "drop", "create", "alter", "copy",
            "install", "load", "attach", "export", "pragma", "truncate",
            "grant", "revoke", "merge", "call",
        }

        if first_keyword not in allowed_keywords:
            raise ValueError("Only read-oriented SQL statements are allowed.")

        if re.search(r"\b(" + "|".join(sorted(forbidden_keywords)) + r")\b", trailing, flags=re.I):
            raise ValueError("Only read-oriented SQL statements are allowed.")

        return trailing

    @staticmethod
    def _coerce_limit(limit: object, default: int = 10, maximum: int = 100) -> int:
        try:
            value = int(limit)
        except (TypeError, ValueError):
            value = default
        return max(1, min(value, maximum))

    async def execute(self, user: User, intent: UserIntent) -> Any:
        self._require_permission(user, Permission.DATA_READ)

        dispatch = {
            "run_sql": self._run_sql,
            "query_stream": self._query_stream,
            "get_schema": self._get_schema,
            "list_tables": self._list_tables,
            "preview": self._preview,
            "catalog_sources": self._catalog_sources,
        }
        handler = dispatch.get(intent.action)
        if handler is None:
            raise ActionNotFoundError(
                f"DataAdapter does not support action '{intent.action}'. "
                f"Available: {list(dispatch.keys())}"
            )
        return await handler(intent)

    def describe_actions(self) -> list[dict[str, Any]]:
        return [
            {
                "name": "run_sql",
                "description": "Execute a read-only SQL query against Delta Lake.",
                "permission": Permission.DATA_READ.value,
                "protocols": ["rest", "mcp"],
            },
            {
                "name": "query_stream",
                "description": "Execute a read-only SQL query against RisingWave.",
                "permission": Permission.DATA_READ.value,
                "protocols": ["rest", "mcp"],
            },
            {
                "name": "get_schema",
                "description": "Return the schema of a specific Delta table.",
                "permission": Permission.DATA_READ.value,
                "protocols": ["rest", "mcp"],
            },
            {
                "name": "list_tables",
                "description": "List available Delta tables in the Lakehouse.",
                "permission": Permission.DATA_READ.value,
                "protocols": ["rest", "mcp"],
            },
            {
                "name": "preview",
                "description": "Return the first N rows of a table as JSON.",
                "permission": Permission.DATA_READ.value,
                "protocols": ["rest", "mcp"],
            },
            {
                "name": "catalog_sources",
                "description": "Return grouped catalog sources across live and fallback storage families.",
                "permission": Permission.DATA_READ.value,
                "protocols": ["rest", "mcp"],
            },
        ]

    async def _run_sql(self, intent: UserIntent) -> dict:
        """Execute a SQL query against Delta Lake via DuckDB.
        
        Uses a dedicated single-threaded executor to serialize all DuckDB calls,
        which avoids thread-safety issues with concurrent requests.
        """
        sql = self._validate_read_only_sql(intent.parameters.get("sql", ""))
        
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
            raise AdapterExecutionError(
                f"DuckDB execution failed: {e}",
                error_type=type(e).__name__,
                context={"backend": "duckdb"},
            ) from e

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

    async def _query_stream(self, intent: UserIntent) -> dict:
        """Execute a SQL query against RisingWave via the ETL source abstraction."""
        sql = self._validate_read_only_sql(intent.parameters.get("sql", ""))

        def _execute_risingwave():
            from etl.config import config
            from etl.io.sources.risingwave import RisingWaveSource

            source = RisingWaveSource(
                host=config.RISINGWAVE_HOST,
                port=config.RISINGWAVE_PORT,
                user=config.RISINGWAVE_USER,
                password=config.RISINGWAVE_PASSWORD,
                database=config.RISINGWAVE_DATABASE,
                query=sql,
            )
            rows = []
            with source.open() as reader:
                for batch in reader.read_batch():
                    rows.extend(batch)
            return rows

        try:
            loop = asyncio.get_event_loop()
            rows = await loop.run_in_executor(_streamdb_executor, _execute_risingwave)
            columns = list(rows[0].keys()) if rows else []
            values = [[row.get(column) for column in columns] for row in rows]
            return {
                "success": True,
                "mode": "json",
                "backend": "risingwave",
                "columns": columns,
                "rows": values,
                "row_count": len(rows),
            }
        except Exception as e:
            raise AdapterExecutionError(
                f"RisingWave query failed: {e}",
                error_type=type(e).__name__,
                context={"backend": "risingwave"},
            ) from e

    async def _list_tables(self, intent: UserIntent) -> dict:
        """List available Delta tables from Hive Metastore with Redis caching."""
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
                    cached_tables = json.loads(cached)
                    normalized_tables = [
                        self._normalize_table_entry(
                            table,
                            family=self._storage_family_from_table(table),
                            source="cache",
                            source_type="cached_inventory",
                        )
                        for table in cached_tables
                    ]
                    return {"tables": normalized_tables, "source": "cache", "source_family": "lakehouse"}
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
            normalized_tables = [
                self._normalize_table_entry(
                    table,
                    family=self._storage_family_from_table(table, default="lakehouse"),
                    source="hive",
                    source_type="hive_metastore",
                )
                for table in tables
            ]
            
            # Cache the result for 60s
            try:
                async with r:
                    await r.setex(cache_key, 60, json.dumps(normalized_tables))
            except Exception:
                pass
                
            return {"tables": normalized_tables, "source": "hive", "source_family": "lakehouse"}
            
        except Exception as e:
            logger.error(f"Hive Metastore lookup failed: {e}. Falling back to static list.")
            # 3. Fallback to static list
            fallback_tables = [
                self._normalize_table_entry(
                    {"name": "market_data", "path": "s3://delta-lake/bronze/market_data"},
                    family="lakehouse",
                    source="fallback",
                    source_type="static_inventory",
                ),
                self._normalize_table_entry(
                    {"name": "news_sentiment", "path": "s3://delta-lake/bronze/news"},
                    family="lakehouse",
                    source="fallback",
                    source_type="static_inventory",
                ),
            ]
            return {
                "tables": fallback_tables,
                "source": "fallback",
                "source_family": "lakehouse",
                "error": str(e)
            }

    @staticmethod
    def _normalize_table_entry(
        table: dict[str, Any],
        *,
        default_schema: str | None = None,
        family: str | None = None,
        source: str | None = None,
        source_type: str | None = None,
    ) -> dict[str, Any]:
        table = table if isinstance(table, dict) else {"name": str(table)}
        name = str(table.get("name") or table.get("table_name") or table.get("table") or "").strip()
        schema = str(table.get("schema") or table.get("table_schema") or default_schema or "").strip() or None
        path = str(table.get("path") or table.get("table_path") or table.get("location") or "").strip() or None
        qualified_name = str(table.get("qualified_name") or "").strip() or None
        if not qualified_name and schema and name:
            qualified_name = f"{schema}.{name}"
        if not path and qualified_name:
            path = qualified_name

        storage_family = family or DataAdapter._storage_family_from_table({
            **table,
            "name": name,
            "schema": schema,
            "path": path,
            "qualified_name": qualified_name,
            "source": source or table.get("source"),
            "source_type": source_type or table.get("source_type"),
        })

        payload: dict[str, Any] = {
            "name": name or qualified_name or path or "unknown",
            "family": storage_family,
        }
        if schema:
            payload["schema"] = schema
        if path:
            payload["path"] = path
        if qualified_name:
            payload["qualified_name"] = qualified_name
        if source or table.get("source"):
            payload["source"] = source or table.get("source")
        if source_type or table.get("source_type"):
            payload["source_type"] = source_type or table.get("source_type")
        return payload

    async def _catalog_sources(self, intent: UserIntent) -> dict:
        """Return grouped source metadata for the catalog UI."""
        from etl.config import config

        async def discover_hive() -> dict[str, Any]:
            hive_tables = await self._list_tables(intent)
            source = hive_tables.get("source", "fallback")
            tables = [
                self._normalize_table_entry(
                    table,
                    family="lakehouse",
                    source=source,
                    source_type="hive_metastore" if source in {"hive", "cache"} else "static_inventory",
                )
                for table in hive_tables.get("tables", [])
            ]
            status = "available" if source in {"cache", "hive"} else "partial"
            return {
                "id": "lakehouse",
                "label": "Lakehouse / Hive",
                "kind": "live" if source in {"cache", "hive"} else "static",
                "status": status,
                "detail": (
                    "Discovered through the Hive metastore."
                    if source == "hive"
                    else "Served from the gateway cache."
                    if source == "cache"
                    else f"Gateway fallback inventory: {hive_tables.get('error', 'static list')}"
                ),
                "tables": tables,
                "source": source,
                "source_family": "lakehouse",
                "source_type": "hive_metastore" if source in {"hive", "cache"} else "static_inventory",
            }

        async def discover_risingwave() -> dict[str, Any]:
            schema = str(config.RISINGWAVE_SCHEMA or "public").strip() or "public"
            sql = (
                "SELECT table_schema, table_name "
                "FROM information_schema.tables "
                f"WHERE table_schema = '{schema}' "
                "ORDER BY table_name"
            )
            try:
                result = await self._query_stream(
                    UserIntent(
                        domain="data",
                        action="query_stream",
                        parameters={"sql": sql},
                        user_id=intent.user_id,
                        roles=intent.roles,
                    )
                )
                tables = []
                for row in result.get("rows", []) or []:
                    row_map = dict(zip(result.get("columns", []), row))
                    tables.append(
                        self._normalize_table_entry(
                            row_map,
                            default_schema=schema,
                            family="streaming_sql",
                            source="risingwave",
                            source_type="metadata_query",
                        )
                    )
                return {
                    "id": "risingwave",
                    "label": "RisingWave",
                    "kind": "live",
                    "status": "available" if tables else "partial",
                    "detail": f"Discovered from {schema} via a live metadata query.",
                    "tables": tables,
                    "source": "risingwave",
                    "source_family": "streaming_sql",
                    "source_type": "metadata_query",
                }
            except Exception as exc:
                fallback_tables = [
                    self._normalize_table_entry(
                        {"name": os.environ.get("DEMO_RISINGWAVE_SIGNAL_TABLE", "market_pulse_signals"), "schema": schema},
                        default_schema=schema,
                        family="streaming_sql",
                        source="fallback",
                        source_type="static_inventory",
                    ),
                    self._normalize_table_entry(
                        {"name": os.environ.get("DEMO_RISINGWAVE_PRICE_TABLE", "market_pulse_prices"), "schema": schema},
                        default_schema=schema,
                        family="streaming_sql",
                        source="fallback",
                        source_type="static_inventory",
                    ),
                ]
                return {
                    "id": "risingwave",
                    "label": "RisingWave",
                    "kind": "live",
                    "status": "partial",
                    "detail": f"Metadata query unavailable: {exc}",
                    "tables": fallback_tables,
                    "source": "fallback",
                    "source_family": "streaming_sql",
                    "source_type": "static_inventory",
                    "error": str(exc),
                }

        async def discover_timescale() -> dict[str, Any]:
            host = os.environ.get("TIMESCALE_HOST")
            db_name = os.environ.get("TIMESCALE_DB", "etl")
            schema_name = os.environ.get("TIMESCALE_SCHEMA", "public")
            user_name = os.environ.get("TIMESCALE_USER")
            password = os.environ.get("TIMESCALE_PASSWORD")
            port = os.environ.get("TIMESCALE_PORT", "5432")

            if not host or not user_name or not password:
                return {
                    "id": "timescaledb",
                    "label": "TimescaleDB",
                    "kind": "live",
                    "status": "pending",
                    "detail": "TimescaleDB connection settings are incomplete.",
                    "tables": [],
                    "source": "config",
                    "source_family": "postgres",
                    "source_type": "operational_sql",
                }

            def _fetch():
                import psycopg2

                conn = psycopg2.connect(
                    host=host,
                    port=port,
                    dbname=db_name,
                    user=user_name,
                    password=password,
                    connect_timeout=5,
                )
                try:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            """
                            SELECT table_schema, table_name
                            FROM information_schema.tables
                            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                            ORDER BY table_schema, table_name
                            LIMIT 200
                            """
                        )
                        return cursor.fetchall()
                finally:
                    conn.close()

            try:
                loop = asyncio.get_event_loop()
                rows = await loop.run_in_executor(None, _fetch)
                tables = [
                    self._normalize_table_entry(
                        {"schema": row[0], "name": row[1], "qualified_name": f"{row[0]}.{row[1]}"},
                        default_schema=schema_name,
                        family="postgres",
                        source="timescaledb",
                        source_type="information_schema",
                    )
                    for row in rows
                ]
                return {
                    "id": "timescaledb",
                    "label": "TimescaleDB",
                    "kind": "live",
                    "status": "available" if tables else "partial",
                    "detail": "Discovered from the operational store metadata.",
                    "tables": tables,
                    "source": "timescaledb",
                    "source_family": "postgres",
                    "source_type": "information_schema",
                }
            except Exception as exc:
                return {
                    "id": "timescaledb",
                    "label": "TimescaleDB",
                    "kind": "live",
                    "status": "partial",
                    "detail": f"Metadata query unavailable: {exc}",
                    "tables": [],
                    "source": "timescaledb",
                    "source_family": "postgres",
                    "source_type": "information_schema",
                    "error": str(exc),
                }

        live_sources = await asyncio.gather(
            discover_hive(),
            discover_risingwave(),
            discover_timescale(),
        )
        live_only_sources = [source for source in live_sources if source.get("kind") == "live"]
        static_only_sources = [source for source in live_sources if source.get("kind") == "static"]
        if not static_only_sources:
            static_only_sources = [
                {
                    "id": "fallback",
                    "label": "Fallback Catalog",
                    "kind": "static",
                    "status": "planned" if not live_only_sources else "partial",
                    "detail": "Static inventory used when live discovery is unavailable.",
                    "tables": [],
                    "source": "fallback",
                    "source_family": "lakehouse",
                    "source_type": "static_inventory",
                }
            ]
        total_tables = sum(len(source.get("tables", [])) for source in live_sources)
        live_available = sum(1 for source in live_only_sources if source.get("status") == "available")

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "live_sources": live_only_sources,
            "static_sources": static_only_sources,
            "summary": {
                "total_sources": len(live_sources),
                "available_sources": live_available,
                "live_sources": len(live_only_sources),
                "static_sources": len(static_only_sources),
                "total_tables": total_tables,
            },
        }

    async def _preview(self, intent: UserIntent) -> dict:
        """Return first N rows of a Delta table."""
        table_path = intent.parameters.get("table_path")
        limit = self._coerce_limit(intent.parameters.get("limit", 10))
        if not table_path:
            raise ValueError("Parameter 'table_path' is required.")
        self._validate_table_path(table_path)
        preview_intent = UserIntent(
            domain="data", action="run_sql",
            parameters={"sql": f"SELECT * FROM delta_scan('{table_path}') LIMIT {limit}"},
            user_id=intent.user_id, roles=intent.roles,
        )
        return await self._run_sql(preview_intent)
