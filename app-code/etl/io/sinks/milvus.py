from dataclasses import dataclass, field
from typing import Optional, Dict, Any, Union, List
import pandas as pd
import pyarrow as pa

from etl.io.base import DataSink, DataWriter

@dataclass
class MilvusSink(DataSink):
    """
    Configuration for writing to Milvus Vector Database.
    """
    REQUIRED_DEPENDENCIES = ["pymilvus"]

    uri: str = "http://localhost:19530" # Or host/port separation
    collection_name: str = "default_collection"
    token: Optional[str] = None # For Zilliz Cloud or Auth
    user: Optional[str] = None
    password: Optional[str] = None
    db_name: str = "default"
    
    # Write behavior
    batch_size: int = 1000
    
    def open(self) -> 'MilvusWriter':
        return MilvusWriter(self)


class MilvusWriter(DataWriter):
    """
    Runtime writer for Milvus.
    """

    def __init__(self, sink: MilvusSink):
        self.sink = sink
        self._conn = None
        self._collection = None

    def _connect(self):
        if self._conn:
            return
            
        try:
            from pymilvus import connections, Collection, utility
        except ImportError:
            raise ImportError("MilvusSink requires 'pymilvus'. Please install it.")

        # connection args
        conn_args = {"uri": self.sink.uri}
        if self.sink.token:
            conn_args["token"] = self.sink.token
        if self.sink.user:
            conn_args["user"] = self.sink.user
            conn_args["password"] = self.sink.password
        if self.sink.db_name:
            conn_args["db_name"] = self.sink.db_name

        # Connect using "default" alias
        connections.connect(alias="default", **conn_args)
        
        # Check collection existence
        if not utility.has_collection(self.sink.collection_name):
            # We do NOT auto-create collections because Vector DB schemas (dim, metric) 
            # are too complex to infer reliably from a DataFrame. 
            raise ValueError(f"[MilvusWriter] Collection '{self.sink.collection_name}' does not exist. Please create it first.")
            
        self._collection = Collection(self.sink.collection_name)
        self._conn = True

    def close(self):
        if self._conn:
            try:
                from pymilvus import connections
                connections.disconnect("default")
            except Exception:
                pass
            self._conn = None
            self._collection = None

    def write_batch(self, data: Union[pd.DataFrame, pa.Table, List[Dict[str, Any]]]):
        """
        Write batch to Milvus.
        """
        self._connect()
        
        # Normalize to List of Dicts or Pandas
        # Milvus Python SDK supports Pandas DataFrame directly usually, 
        # or list of dicts.
        
        target_data = data
        
        if isinstance(data, pa.Table):
            target_data = data.to_pandas()
        elif isinstance(data, list):
            # Keep as list of dicts
            pass
            
        try:
            # Insert returns a MutationResult
            res = self._collection.insert(target_data)
            # Milvus insert is async-ish, data is not immediately searchable until flush/index
            # But for sink purposes, insert is enough.
            print(f"[MilvusWriter] Inserted {res.insert_count} entities into {self.sink.collection_name}")
            
        except Exception as e:
            print(f"[MilvusWriter] Error writing to {self.sink.collection_name}: {e}")
            raise
