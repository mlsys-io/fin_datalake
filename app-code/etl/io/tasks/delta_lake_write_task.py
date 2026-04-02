"""
Delta Lake Write Task - BaseTask implementation for Delta Lake writes.

Now uses Ray Data for distributed writes to avoid Tokio/Ray conflicts.
Supports both distributed (.submit()) and local (.local()) execution.
"""
from typing import Any, Dict, List, Optional

from etl.core.base_task import BaseTask
from etl.io.sinks.delta_lake import DeltaLakeSink


class DeltaLakeWriteTask(BaseTask):
    """
    Task for writing data to Delta Lake.

    Distributed Execution (Recommended):
        Uses Ray Data's write_deltalake() which handles Tokio runtime safely.

    Local Execution:
        Use .local() to run on driver for small datasets or debugging.

    Usage:
        task = DeltaLakeWriteTask(
            name="Write to Bronze",
            uri="s3://delta-lake/bronze/table",
        )

        # Distributed - uses Ray Data (recommended)
        task.submit(data)

        # Local - runs on driver
        task.local(data)
    """

    def __init__(
        self,
        name: str = "Write to Delta Lake",
        uri: str = "",
        mode: str = "append",
        partition_by: Optional[List[str]] = None,
        hive_table_name: Optional[str] = None,
        hive_config: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        """
        Args:
            name: Task name
            uri: Delta Lake table URI (e.g., s3://bucket/path)
            mode: Write mode - "append" or "overwrite"
            partition_by: Optional list of partition columns
            hive_table_name: Optional Hive table name for registration
            hive_config: Optional Hive Metastore connection config
        """
        super().__init__(name=name, **kwargs)
        self.uri = uri
        self.mode = mode
        self.partition_by = partition_by
        self.hive_table_name = hive_table_name
        self.hive_config = hive_config

    def run(self, data: Any, uri: Optional[str] = None, use_ray_data: bool = True) -> str:
        """
        Write data to Delta Lake.

        Args:
            data: DataFrame, PyArrow Table, Ray Dataset, or list of dicts
            uri: Override URI (optional, uses instance URI if not provided)
            use_ray_data: If True, convert to Ray Dataset for distributed write (default: True)

        Returns:
            Status message
        """
        import pandas as pd

        from etl.config import config

        target_uri = uri or self.uri
        if not target_uri:
            raise ValueError("Delta Lake URI is required")

        sink = DeltaLakeSink(
            uri=target_uri,
            aws_access_key_id=config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
            endpoint_url=config.AWS_ENDPOINT_URL,
            region=config.AWS_REGION,
            mode=self.mode,
            partition_by=self.partition_by,
            hive_table_name=self.hive_table_name,
            hive_config=self.hive_config,
        )

        writer = sink.open()
        try:
            if use_ray_data:
                import ray.data as rd

                ds = data
                if not isinstance(data, rd.Dataset):
                    if isinstance(data, pd.DataFrame):
                        ds = rd.from_pandas(data)
                    elif isinstance(data, list):
                        ds = rd.from_items(data)
                    else:
                        ds = rd.from_pandas(pd.DataFrame(data))

                writer.write_dataset(ds)
                return f"Successfully wrote dataset to {target_uri} (distributed)"

            if isinstance(data, list):
                data = pd.DataFrame(data)

            writer.write_batch(data)
            row_count = len(data) if hasattr(data, "__len__") else "unknown"
            return f"Wrote {row_count} records to {target_uri} (local)"
        finally:
            writer.close()

    def local(self, *args, **kwargs):
        """
        Execute task locally on the driver.
        Forces use_ray_data=False to bypass Ray Dataset conversion.
        """
        kwargs["use_ray_data"] = False
        return super().local(*args, **kwargs)
