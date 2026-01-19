from dataclasses import dataclass
from typing import Optional, Dict, Any, Union, List
import pandas as pd
import pyarrow as pa

from etl.io.base import DataSink, DataWriter

@dataclass
class TimescaleDBSink(DataSink):
    """
    Configuration for writing to TimescaleDB (PostgreSQL).
    """
    REQUIRED_DEPENDENCIES = ["psycopg2"]

    host: str
    user: str
    password: str
    database: str
    table_name: str
    port: int = 5432
    schema: str = "public"
    
    # Write behavior
    on_conflict_do_nothing: bool = False

    def open(self) -> 'TimescaleDBWriter':
        return TimescaleDBWriter(self)


class TimescaleDBWriter(DataWriter):
    """
    Runtime writer for TimescaleDB using Psycopg2.
    """

    def __init__(self, sink: TimescaleDBSink):
        self.sink = sink
        self._conn = None
        self._cursor = None

    def _connect(self):
        if self._conn:
            return

        try:
            import psycopg2
        except ImportError:
            raise ImportError("TimescaleDBWriter requires 'psycopg2' or 'psycopg2-binary'. Please install it.")

        self._conn = psycopg2.connect(
            host=self.sink.host,
            port=self.sink.port,
            user=self.sink.user,
            password=self.sink.password,
            dbname=self.sink.database
        )
        self._cursor = self._conn.cursor()

    def close(self):
        if self._cursor:
            self._cursor.close()
        if self._conn:
            self._conn.close()
        self._cursor = None
        self._conn = None

    def write_batch(self, data: Union[pd.DataFrame, pa.Table, List[Dict[str, Any]]]):
        """
        Writes a batch of data to the target table using execute_values.
        """
        self._connect()
        from psycopg2.extras import execute_values

        # Normalize to list of dicts or list of tuples
        records = []
        columns = []

        if isinstance(data, pd.DataFrame):
            columns = list(data.columns)
            records = data.to_records(index=False).tolist() # List of tuples
        elif isinstance(data, pa.Table):
            columns = data.column_names
            records = data.to_pylist() # List of dicts, careful with order
            # Convert list of dicts to list of tuples for execute_values efficiency
            records = [tuple(r[c] for c in columns) for r in records]
        elif isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
            columns = list(data[0].keys())
            records = [tuple(r.get(c, None) for c in columns) for r in data]
        else:
            if not data:
                return # Empty batch
            raise ValueError(f"Unsupported data type for TimescaleDBWriter: {type(data)}")

        if not records:
            return

        # Build SQL
        full_table_name = f"{self.sink.schema}.{self.sink.table_name}"
        cols_str = ",".join([f'"{c}"' for c in columns])
        
        sql = f"INSERT INTO {full_table_name} ({cols_str}) VALUES %s"
        if self.sink.on_conflict_do_nothing:
            sql += " ON CONFLICT DO NOTHING"

        try:
            execute_values(self._cursor, sql, records)
            self._conn.commit()
            print(f"[TimescaleDBWriter] Wrote {len(records)} rows to {full_table_name}")
        except Exception as e:
            self._conn.rollback()
            print(f"[TimescaleDBWriter] Error writing to {full_table_name}: {e}")
            raise
