"""
RisingWave sink for writing rows over the PostgreSQL wire protocol.
"""
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Union
import json

from etl.io.base import DataSink, DataWriter

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa


@dataclass
class RisingWaveSink(DataSink):
    REQUIRED_DEPENDENCIES = ["psycopg2"]

    host: str
    user: str
    password: str
    database: str
    table_name: str
    port: int = 4566
    schema: str = "public"
    ddl: Optional[str] = None
    on_conflict_clause: Optional[str] = None

    def open(self) -> "RisingWaveWriter":
        return RisingWaveWriter(self)


class RisingWaveWriter(DataWriter):
    def __init__(self, sink: RisingWaveSink):
        self.sink = sink
        self._conn = None
        self._cursor = None
        self._ddl_applied = False

    def _connect(self):
        if self._conn:
            return

        try:
            import psycopg2
        except ImportError:
            raise ImportError("RisingWaveWriter requires 'psycopg2' or 'psycopg2-binary'. Please install it.")

        self._conn = psycopg2.connect(
            host=self.sink.host,
            port=self.sink.port,
            user=self.sink.user,
            password=self.sink.password,
            dbname=self.sink.database,
        )
        self._cursor = self._conn.cursor()

    def _ensure_ddl(self):
        if self._ddl_applied or not self.sink.ddl:
            return
        self._cursor.execute(self.sink.ddl)
        self._conn.commit()
        self._ddl_applied = True

    def close(self):
        if self._cursor:
            self._cursor.close()
        if self._conn:
            self._conn.close()
        self._cursor = None
        self._conn = None

    def _normalize_records(self, data: Union[Any, Any, List[Dict[str, Any]]]) -> tuple[list[str], list[tuple[Any, ...]]]:
        import pandas as pd
        import pyarrow as pa

        records: list[tuple[Any, ...]] = []
        columns: list[str] = []

        if isinstance(data, pd.DataFrame):
            columns = list(data.columns)
            records = data.to_records(index=False).tolist()
        elif isinstance(data, pa.Table):
            columns = list(data.column_names)
            rows = data.to_pylist()
            records = [tuple(row.get(col) for col in columns) for row in rows]
        elif isinstance(data, list) and data and isinstance(data[0], dict):
            columns = list(data[0].keys())
            normalized_rows = []
            for row in data:
                normalized = []
                for col in columns:
                    value = row.get(col)
                    if isinstance(value, (dict, list)):
                        value = json.dumps(value, default=str)
                    normalized.append(value)
                normalized_rows.append(tuple(normalized))
            records = normalized_rows
        else:
            if not data:
                return [], []
            raise ValueError(f"Unsupported data type for RisingWaveWriter: {type(data)}")

        return columns, records

    def write_batch(self, data: Union[Any, Any, List[Dict[str, Any]]]):
        from psycopg2.extras import execute_values

        self._connect()
        self._ensure_ddl()

        columns, records = self._normalize_records(data)
        if not columns or not records:
            return

        quoted_schema = f'"{self.sink.schema}"'
        quoted_table = f'"{self.sink.table_name}"'
        cols_str = ",".join(f'"{column}"' for column in columns)
        sql = f"INSERT INTO {quoted_schema}.{quoted_table} ({cols_str}) VALUES %s"
        if self.sink.on_conflict_clause:
            sql += f" {self.sink.on_conflict_clause}"

        try:
            execute_values(self._cursor, sql, records)
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
