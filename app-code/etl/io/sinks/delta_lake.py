"""
Delta Lake Sink for writing data to Delta Lake tables.
Heavy imports are deferred to runtime for Ray worker execution.
"""
from contextlib import contextmanager
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
    dlq_uri: Optional[str] = None  # Route failed batches to this URI instead of failing
    
    
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
        return {k: str(v) for k, v in opts.items() if v is not None}

    @contextmanager
    def _delta_ssl_env(self):
        """Temporarily apply MinIO/internal CA settings only around Delta operations."""
        import os
        from loguru import logger
        from etl.config import config
        
        ca_path = getattr(config, 'CA_PATH', '/opt/certs/public.crt')
        keys = ("SSL_CERT_FILE", "AWS_CA_BUNDLE", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE")
        previous = {key: os.environ.get(key) for key in keys}
        previous_metadata_flag = os.environ.get("AWS_EC2_METADATA_DISABLED")

        try:
            if ca_path and os.path.exists(ca_path):
                for key in keys:
                    os.environ[key] = ca_path
                logger.info(f"[DeltaLake] SSL certificate configured for Delta operations: {ca_path}")
            else:
                logger.warning(f"[DeltaLake] SSL certificate not found at: {ca_path}")
            os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
            yield
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

            if previous_metadata_flag is None:
                os.environ.pop("AWS_EC2_METADATA_DISABLED", None)
            else:
                os.environ["AWS_EC2_METADATA_DISABLED"] = previous_metadata_flag

    def write_dataset(self, ds: "ray.data.Dataset") -> None:
        """
        Write a Ray Dataset to Delta Lake (distributed execution).
        
        Uses Ray's native write_deltalake which handles Tokio runtime internally.
        This avoids conflicts with Ray's async loop.
        
        Args:
            ds: Ray Dataset to write
        """
        from loguru import logger
        
        logger.info(f"[DeltaLake] Writing Ray Dataset to {self.sink.uri} (Distributed)...")
        
        try:
            with self._delta_ssl_env():
                self._write_ray_dataset(ds)
            logger.success(f"[DeltaLake] ✅ Successfully wrote dataset to {self.sink.uri}")
        except Exception as e:
            logger.error(f"[DeltaLake] Failed to write Ray Dataset: {e}")
            raise

    def write_batch(self, data: Union[Any, Any, Any]):
        """
        Write a batch (DataFrame, Table, or Ray Dataset) to Delta Lake.
        """
        # Heavy imports - only executed on Ray workers
        import pyarrow as pa
        import pandas as pd
        from loguru import logger
        
        try:
            with self._delta_ssl_env():
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

        if self.sink.mode == "overwrite":
            logger.info(
                f"[DeltaLake] Coalescing dataset to a single block before overwrite for {self.sink.uri}."
            )
            ds = ds.repartition(1, shuffle=False)
        
        class _RayDeltaSink(rd.Datasink):
            def __init__(self, uri_root, mode, storage_opts, ca_path):
                self.uri_root = uri_root
                self.mode = mode
                self.storage_opts = storage_opts
                self.ca_path = ca_path
            
            def write(self, blocks, ctx):
                import os

                keys = ("SSL_CERT_FILE", "AWS_CA_BUNDLE", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE")
                previous = {key: os.environ.get(key) for key in keys}
                previous_metadata_flag = os.environ.get("AWS_EC2_METADATA_DISABLED")

                try:
                    if self.ca_path and os.path.exists(self.ca_path):
                        for key in keys:
                            os.environ[key] = self.ca_path
                    os.environ["AWS_EC2_METADATA_DISABLED"] = "true"

                    for block in blocks:
                        if isinstance(block, pd.DataFrame):
                            block = pa.Table.from_pandas(block, preserve_index=False)
                        
                        write_deltalake(
                            self.uri_root, 
                            block, 
                            mode=self.mode, 
                            storage_options=self.storage_opts
                        )
                finally:
                    for key, value in previous.items():
                        if value is None:
                            os.environ.pop(key, None)
                        else:
                            os.environ[key] = value

                    if previous_metadata_flag is None:
                        os.environ.pop("AWS_EC2_METADATA_DISABLED", None)
                    else:
                        os.environ["AWS_EC2_METADATA_DISABLED"] = previous_metadata_flag

        from etl.config import config
        sink = _RayDeltaSink(
            self.sink.uri,
            self.sink.mode,
            self._storage_options,
            getattr(config, "CA_PATH", "/opt/certs/public.crt"),
        )
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
        
        try:
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
                
        except Exception as write_err:
            if self.sink.dlq_uri:
                logger.warning(f"[DeltaLake] Write failed: {write_err}. Routing to DLQ ({self.sink.dlq_uri}).")
                from etl.io.sinks.dlq import DeadLetterQueue
                dlq = DeadLetterQueue(self.sink.dlq_uri, self._storage_options)
                dlq.send(data, str(write_err))
            else:
                logger.error(f"[DeltaLake] Write failed and no DLQ configured: {write_err}")
                raise

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
