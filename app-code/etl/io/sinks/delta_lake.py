"""
Delta Lake Sink for writing data to Delta Lake tables.
Heavy imports are deferred to runtime for Ray worker execution.
"""
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, Union, List, TYPE_CHECKING

from etl.io.base import DataSink, DataWriter

# Type hints only - not imported at runtime on local machine
if TYPE_CHECKING:
    import pyarrow as pa
    import pandas as pd
    from etl.services.hive import HiveMetastore


@dataclass
class DeltaLakeSink(DataSink):
    """
    Configuration for writing to Delta Lake.
    """
    REQUIRED_DEPENDENCIES = ["deltalake", "pyarrow", "pandas", "thrift"]

    uri: str
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    endpoint_url: Optional[str] = None
    region: str = "us-east-1"
    allow_unsafe_rename: bool = True
    storage_options_extra: Dict[str, str] = field(default_factory=dict)
    
    # Write behaviors
    mode: str = "append"  # "append" or "overwrite"
    partition_by: Optional[List[str]] = None
    schema_mode: Optional[str] = None  # "merge", "overwrite", or None
    
    # Optional Integration - store as dict for serialization
    hive_config: Optional[Dict[str, Any]] = None  # {"host": "...", "port": 9083}
    hive_table_name: Optional[str] = None  # e.g. "default.my_table"
    
    def open(self) -> 'DeltaLakeWriter':
        return DeltaLakeWriter(self)


class DeltaLakeWriter(DataWriter):
    """
    Runtime writer for Delta Lake.
    Heavy imports happen here, not at module level.
    """

    def __init__(self, sink: DeltaLakeSink):
        self.sink = sink
        self._storage_options = self._build_storage_options()

    def _build_storage_options(self) -> Dict[str, str]:
        opts = {
            "AWS_ACCESS_KEY_ID": self.sink.aws_access_key_id,
            "AWS_SECRET_ACCESS_KEY": self.sink.aws_secret_access_key,
            "AWS_REGION": self.sink.region,
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true" if self.sink.allow_unsafe_rename else "false",
        }
        if self.sink.endpoint_url:
            opts["AWS_ENDPOINT_URL"] = self.sink.endpoint_url
            opts["AWS_S3_FORCE_PATH_STYLE"] = "true"
            
        opts.update(self.sink.storage_options_extra)
        return opts

    def _ensure_ssl_cert(self):
        """Set SSL_CERT_FILE for Rust object_store if certificate exists."""
        import os
        from loguru import logger
        from etl.config import config
        
        ca_path = getattr(config, 'CA_PATH', '/opt/certs/public.crt')
        if ca_path and os.path.exists(ca_path):
            os.environ.setdefault("SSL_CERT_FILE", ca_path)
            logger.info(f"[DeltaLake] SSL certificate configured: {ca_path}")
        else:
            logger.warning(f"[DeltaLake] SSL certificate not found at: {ca_path}")
        os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")

    def write_dataset(self, ds: "ray.data.Dataset") -> None:
        """
        Write a Ray Dataset to Delta Lake (distributed execution).
        
        Uses Ray's native write_deltalake which handles Tokio runtime internally.
        This avoids conflicts with Ray's async loop.
        
        Args:
            ds: Ray Dataset to write
        """
        self._ensure_ssl_cert()
        from loguru import logger
        
        logger.info(f"[DeltaLake] Writing Ray Dataset to {self.sink.uri} (Distributed)...")
        
        try:
            ds.write_deltalake(
                self.sink.uri,
                mode=self.sink.mode,
                storage_options=self._storage_options,
            )
            logger.success(f"[DeltaLake] ✅ Successfully wrote dataset to {self.sink.uri}")
        except Exception as e:
            logger.error(f"[DeltaLake] Failed to write Ray Dataset: {e}")
            raise

        # Hive Registration
        if self.sink.hive_config and self.sink.hive_table_name:
            self._register_in_hive(ds.schema())

    def write_batch(self, data: Union[Any, Any, Any]):
        """
        Write a batch (DataFrame, Table, or Ray Dataset) to Delta Lake.
        """
        # Ensure SSL certificate is set for HTTPS MinIO connections
        self._ensure_ssl_cert()
        
        # Heavy imports - only executed on Ray workers
        import pyarrow as pa
        import pandas as pd
        from loguru import logger
        
        try:
            # HACK: Lazy import ray to check type without hard dependency if not used
            is_ray_ds = False
            try:
                import ray.data as rd
                if isinstance(data, rd.Dataset):
                    is_ray_ds = True
            except ImportError:
                pass

            if is_ray_ds:
                self._write_ray_dataset(data)
            else:
                self._write_local_batch(data)
        except Exception as e:
            logger.error(f"Error writing to {self.sink.uri}: {e}")
            raise

    def _write_ray_dataset(self, ds):
        """
        Distributed write using Ray Datasink pattern.
        """
        import pyarrow as pa
        import pandas as pd
        import ray.data as rd
        from deltalake import write_deltalake
        from loguru import logger
        
        class _RayDeltaSink(rd.Datasink):
            def __init__(self, uri_root, mode, storage_opts):
                self.uri_root = uri_root
                self.mode = mode
                self.storage_opts = storage_opts
            
            def write(self, blocks, ctx):
                for block in blocks:
                    if isinstance(block, pd.DataFrame):
                        block = pa.Table.from_pandas(block, preserve_index=False)
                    
                    write_deltalake(
                        self.uri_root, 
                        block, 
                        mode=self.mode, 
                        storage_options=self.storage_opts
                    )

        sink = _RayDeltaSink(self.sink.uri, self.sink.mode, self._storage_options)
        ds.write_datasink(sink)
        
        # Auto-Register in Hive
        if self.sink.hive_config and self.sink.hive_table_name:
            arrow_schema = ds.schema() 
            self._register_in_hive(arrow_schema)

    def _write_local_batch(self, data):
        """Write a local DataFrame or PyArrow Table."""
        import pyarrow as pa
        import pandas as pd
        from deltalake import write_deltalake
        from loguru import logger
        
        if isinstance(data, pd.DataFrame):
            data = pa.Table.from_pandas(data, preserve_index=False)
            
        # Schema Cleaning (Timezone stripping)
        data = self._clean_schema(data)
        
        logger.info(f"[DeltaLake] Writing {data.num_rows} rows to {self.sink.uri}...")
        
        write_deltalake(
            table_or_uri=self.sink.uri,
            data=data,
            mode=self.sink.mode,
            partition_by=self.sink.partition_by,
            storage_options=self._storage_options,
            schema_mode=self.sink.schema_mode or ("overwrite" if self.sink.mode == "overwrite" else "merge")
        )
        
        logger.success(f"[DeltaLake] ✅ Successfully wrote {data.num_rows} rows to {self.sink.uri}")
        
        if self.sink.hive_config and self.sink.hive_table_name:
            self._register_in_hive(data.schema)

    def _register_in_hive(self, schema):
        """Register Delta table in Hive Metastore."""
        from loguru import logger
        from etl.services.hive import HiveMetastore
        
        try:
            db, table = "default", self.sink.hive_table_name
            if "." in self.sink.hive_table_name:
                db, table = self.sink.hive_table_name.split(".", 1)
            
            # Create HiveMetastore from config dict
            hms = HiveMetastore(**self.sink.hive_config)
                
            with hms.open() as client:
                client.register_delta_table(
                    db_name=db,
                    table_name=table,
                    location=self.sink.uri,
                    schema=schema,
                    partition_keys=self.sink.partition_by or []
                )
        except Exception as e:
            logger.warning(f"Failed to register table in Hive: {e}")

    def _clean_schema(self, table):
        """Strip timezone information from timestamp fields for Delta Lake compatibility."""
        import pyarrow as pa
        
        new_fields = []
        schema = table.schema
        changed = False
        
        for f in schema:
            t = f.type
            if pa.types.is_timestamp(t) and getattr(t, "tz", None):
                t = pa.timestamp(t.unit)
                changed = True
            new_fields.append(pa.field(f.name, t, f.nullable, f.metadata))
            
        if changed:
            return table.cast(pa.schema(new_fields))
        return table

    def close(self):
        # Delta writer is stateless per write call, nothing to close.
        pass
