"""
Task for writing row-oriented data into RisingWave.
"""
from typing import Any, Optional

from etl.config import config
from etl.core.base_task import BaseTask
from etl.io.sinks.risingwave import RisingWaveSink


class RisingWaveWriteTask(BaseTask):
    def __init__(
        self,
        name: str = "Write to RisingWave",
        table_name: str = "",
        schema: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        ddl: Optional[str] = None,
        on_conflict_clause: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(name=name, **kwargs)
        self.table_name = table_name
        self.schema = schema or config.RISINGWAVE_SCHEMA
        self.host = host or config.RISINGWAVE_HOST
        self.port = int(port or config.RISINGWAVE_PORT)
        self.user = user or config.RISINGWAVE_USER
        self.password = password if password is not None else config.RISINGWAVE_PASSWORD
        self.database = database or config.RISINGWAVE_DATABASE
        self.ddl = ddl
        self.on_conflict_clause = on_conflict_clause

    def run(
        self,
        data: Any,
        *,
        table_name: Optional[str] = None,
        ddl: Optional[str] = None,
    ) -> str:
        target_table = str(table_name or self.table_name).strip()
        if not target_table:
            raise ValueError("RisingWave table_name is required")
        if not str(self.host).strip():
            raise ValueError("RisingWave host is required")

        sink = RisingWaveSink(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            schema=self.schema,
            table_name=target_table,
            ddl=ddl or self.ddl,
            on_conflict_clause=self.on_conflict_clause,
        )

        with sink.open() as writer:
            writer.write_batch(data)

        row_count = len(data) if hasattr(data, "__len__") else "unknown"
        return f"Wrote {row_count} records to {self.schema}.{target_table}"
