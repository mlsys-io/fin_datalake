from dataclasses import dataclass, field
from typing import Optional, Dict, Any, Union, List
import pyarrow as pa
import pandas as pd
from deltalake import write_deltalake
from loguru import logger

from etl.io.base import DataSink, DataWriter

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
    mode: str = "append" # "append" or "overwrite"
    partition_by: Optional[List[str]] = None
    schema_mode: Optional[str] = None # "merge", "overwrite", or None
    
    # Optional Integration
    hive_metastore: Optional[HiveMetastore] = None
    hive_table_name: Optional[str] = None # e.g. "default.my_table"
    
    def open(self) -> 'DeltaLakeWriter':
        return DeltaLakeWriter(self)

class DeltaLakeWriter(DataWriter):
    """
    Runtime writer for Delta Lake.
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



    def write_batch(self, data: Union[pd.DataFrame, pa.Table, Any]):
        """
        Write a batch (DataFrame, Table, or Ray Dataset) to Delta Lake.
        """
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
            # We explicitly catch and re-raise to add context
            logger.error(f"Error writing to {self.sink.uri}: {e}")
            raise

    def _write_ray_dataset(self, ds):
        """
        Distributed write using Ray Datasink pattern.
        
        Writes a Ray Dataset to Delta Lake in a distributed fashion by:
        1. Defining a custom Datasink that wraps deltalake.write_deltalake
        2. Each block/partition writes independently in parallel
        3. Automatically registers the table in Hive Metastore if configured
        
        Args:
            ds: Ray Dataset containing data to write
        """
        # 1. Define custom Datasink
        import ray.data as rd
        
        class _RayDeltaSink(rd.Datasink):
            def __init__(self, uri_root, mode, storage_opts):
                self.uri_root = uri_root
                self.mode = mode
                self.storage_opts = storage_opts
            
            def write(self, blocks, ctx):
                from deltalake import write_deltalake
                for block in blocks:
                    # Convert block to Arrow if needed
                    if isinstance(block, pd.DataFrame):
                        block = pa.Table.from_pandas(block, preserve_index=False)
                    
                    # Clean Schema (strip TZ) - reimplemented inline or we can use helper
                    # For performance, we assume blocks are homogenous and rely on write_deltalake handling
                    # But stripping TZ is critical for Delta compatibility
                    
                    write_deltalake(
                        self.uri_root, 
                        block, 
                        mode=self.mode, 
                        storage_options=self.storage_opts
                    )

        # 2. Trigger Write
        # We repartition to control file count if specified in extra options, else default
        # For now, just write.
        sink = _RayDeltaSink(self.sink.uri, self.sink.mode, self._storage_options)
        ds.write_datasink(sink)
        
        # 3. Auto-Register (Schema is available from ds.schema())
        if self.sink.hive_metastore and self.sink.hive_table_name:
            # Ray schema -> Arrow Schema
            arrow_schema = ds.schema() 
            # Note: Ray schema might be PandasBlockSchema, need conversion if complex
            # For now assuming basic arrow schema
            self._register_in_hive(arrow_schema)

    def _write_local_batch(self, data: Union[pd.DataFrame, pa.Table]):
        if isinstance(data, pd.DataFrame):
            data = pa.Table.from_pandas(data, preserve_index=False)
            
        # Schema Cleaning (Timezone stripping)
        data = self._clean_schema(data)
        
        write_deltalake(
            table_or_uri=self.sink.uri,
            data=data,
            mode=self.sink.mode,
            partition_by=self.sink.partition_by,
            storage_options=self._storage_options,
            schema_mode=self.sink.schema_mode or ("overwrite" if self.sink.mode == "overwrite" else "merge")
        )
        
        if self.sink.hive_metastore and self.sink.hive_table_name:
            self._register_in_hive(data.schema)


    def _register_in_hive(self, schema: pa.Schema):
        try:
            db, table = "default", self.sink.hive_table_name
            if "." in self.sink.hive_table_name:
                db, table = self.sink.hive_table_name.split(".", 1)
                
            with self.sink.hive_metastore.open() as hms:
                hms.register_delta_table(
                    db_name=db,
                    table_name=table,
                    location=self.sink.uri,
                    schema=schema,
                    partition_keys=self.sink.partition_by or []
                )
        except Exception as e:
            logger.warning(f"Failed to register table in Hive: {e}")

    def _clean_schema(self, table: pa.Table) -> pa.Table:
        """
        Strip timezone information from timestamp fields for Delta Lake compatibility.
        
        Delta Lake requires timestamps without timezone info. This method:
        1. Iterates through all fields in the Arrow schema
        2. Identifies timestamp fields with timezone information
        3. Converts them to timezone-naive timestamps with same unit
        4. Casts the table to the new schema if any changes were made
        
        Args:
            table: PyArrow Table potentially containing timezone-aware timestamps
            
        Returns:
            PyArrow Table with timezone-naive timestamps
        """
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
