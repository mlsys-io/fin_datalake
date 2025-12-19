from dataclasses import dataclass, field
from typing import Optional, Dict, Any, Union, List
import pyarrow as pa
import pandas as pd
from deltalake import write_deltalake

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
    REQUIRED_DEPENDENCIES = ["deltalake", "pyarrow", "pandas", "thrift"]

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

    def write_batch(self, data: Union[pd.DataFrame, pa.Table]):
        """
        Write a batch (DataFrame or Table) to Delta Lake.
        """
        if isinstance(data, pd.DataFrame):
            data = pa.Table.from_pandas(data, preserve_index=False)
            
        # Schema Cleaning (Timezone stripping)
        data = self._clean_schema(data)
        
        try:
            write_deltalake(
                table_or_uri=self.sink.uri,
                data=data,
                mode=self.sink.mode,
                partition_by=self.sink.partition_by,
                storage_options=self._storage_options,
                schema_mode=self.sink.schema_mode or ("overwrite" if self.sink.mode == "overwrite" else "merge")
            )
            
            # --- Foolproof Integration: Auto-Register in Hive ---
            if self.sink.hive_metastore and self.sink.hive_table_name:
                self._register_in_hive(data.schema)
                
        except Exception as e:
            # We explicitly catch and re-raise to add context
            print(f"[DeltaLakeWriter] Error writing to {self.sink.uri}: {e}")
            raise

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
            print(f"[DeltaLakeWriter] Warning: Failed to register in Hive: {e}")

    def _clean_schema(self, table: pa.Table) -> pa.Table:
        """Strip timezones from timestamps for compatibility."""
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
