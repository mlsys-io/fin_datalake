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
from gateway.models.intent import UserIntent
from gateway.models.user import Permission, User


class DataAdapter(BaseAdapter):

    def handles(self) -> str:
        return "data"

    def execute(self, user: User, intent: UserIntent) -> Any:
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
        return handler(intent)

    def _run_sql(self, intent: UserIntent) -> dict:
        """Execute a SQL query against Delta Lake via DuckDB."""
        import duckdb
        sql = intent.parameters.get("sql")
        if not sql:
            raise ValueError("Parameter 'sql' is required.")
        conn = duckdb.connect()
        result = conn.execute(sql).fetchdf()
        return {
            "columns": list(result.columns),
            "rows": result.values.tolist(),
            "row_count": len(result),
        }

    def _get_schema(self, intent: UserIntent) -> dict:
        """Return the schema fields of a Delta table."""
        from deltalake import DeltaTable
        table_path = intent.parameters.get("table_path")
        if not table_path:
            raise ValueError("Parameter 'table_path' is required.")
        dt = DeltaTable(table_path)
        schema = dt.schema()
        return {"table_path": table_path, "fields": [
            {"name": f.name, "type": str(f.type)} for f in schema.fields
        ]}

    def _list_tables(self, intent: UserIntent) -> dict:
        """List available Delta tables (TODO: integrate Live Data Catalog)."""
        return {
            "tables": [
                {"name": "market_data", "path": "s3://delta-lake/bronze/market_data"},
                {"name": "news_sentiment", "path": "s3://delta-lake/bronze/news"},
            ],
            "note": "Live catalog integration pending (see DESIGN_ETL_ENHANCEMENTS.md)."
        }

    def _preview(self, intent: UserIntent) -> dict:
        """Return first N rows of a Delta table."""
        table_path = intent.parameters.get("table_path")
        limit = intent.parameters.get("limit", 10)
        if not table_path:
            raise ValueError("Parameter 'table_path' is required.")
        preview_intent = UserIntent(
            domain="data", action="run_sql",
            parameters={"sql": f"SELECT * FROM delta_scan('{table_path}') LIMIT {limit}"},
            user_id=intent.user_id, role=intent.role,
        )
        return self._run_sql(preview_intent)
