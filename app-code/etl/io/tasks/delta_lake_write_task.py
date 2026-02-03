"""
Delta Lake Write Task - BaseTask implementation for Delta Lake writes.

IMPORTANT: Due to Tokio runtime conflicts with Ray, this task should be 
called with .local() instead of regular distributed execution.

See: etl/docs/RUNTIME_CONFLICTS.md for details.
"""
from typing import Any, Dict, List, Optional, Union
from etl.core.base_task import BaseTask
from etl.io.sinks.delta_lake import DeltaLakeSink


class DeltaLakeWriteTask(BaseTask):
    """
    Task for writing data to Delta Lake.
    
    WARNING: Use .local() due to Tokio/Ray runtime conflict.
    
    Usage:
        task = DeltaLakeWriteTask(
            name="Write to Bronze",
            uri="s3://delta-lake/bronze/table",
        )
        
        # ✅ Correct - runs on driver (avoids Tokio crash)
        task.local(data)
        
        # ❌ Avoid - will crash on Ray workers
        # task(data)
    """
    
    # Mark this task as requiring local execution
    REQUIRES_LOCAL_EXECUTION = True
    CONFLICT_REASON = "Delta Lake uses Tokio (Rust async runtime) which conflicts with Ray"
    
    def __init__(
        self,
        name: str = "Write to Delta Lake",
        uri: str = "",
        mode: str = "append",
        partition_by: Optional[List[str]] = None,
        hive_table_name: Optional[str] = None,
        hive_config: Optional[Dict[str, Any]] = None,
        **kwargs
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
    
    def run(self, data: Any, uri: Optional[str] = None) -> str:
        """
        Write data to Delta Lake.
        
        Args:
            data: DataFrame, PyArrow Table, or list of dicts
            uri: Override URI (optional, uses instance URI if not provided)
            
        Returns:
            Status message
        """
        from etl.config import config
        import pandas as pd
        
        target_uri = uri or self.uri
        if not target_uri:
            raise ValueError("Delta Lake URI is required")
        
        # Convert list of dicts to DataFrame if needed
        if isinstance(data, list):
            data = pd.DataFrame(data)
        
        # Create sink with config
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
        
        # Write using the sink
        writer = sink.open()
        try:
            writer.write_batch(data)
            row_count = len(data) if hasattr(data, '__len__') else 'unknown'
            print(f"[{self.name}] Successfully wrote {row_count} records to {target_uri}")
            return f"Wrote {row_count} records to {target_uri}"
        finally:
            writer.close()
    
    def __call__(self, *args, **kwargs):
        """
        Override to warn about runtime conflict.
        """
        import warnings
        warnings.warn(
            f"{self.name}: Delta Lake has Tokio/Ray conflict. "
            "Consider using .local() instead for reliable execution.",
            RuntimeWarning
        )
        return super().__call__(*args, **kwargs)
