from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Tuple
import pyarrow as pa
from etl.core.utils.dependency_aware_mixin import DependencyAwareMixin

# Thrift Imports (handled dynamically to avoid crashes if missing on driver)
try:
    from thrift.transport import TSocket, TTransport
    from thrift.protocol import TBinaryProtocol
    # Adjust these imports based on your generated code location
    from ioutils.hms_client.hive_metastore import ThriftHiveMetastore as HMSvc
    from ioutils.hms_client.hive_metastore import ttypes as HMS
except ImportError:
    HMSvc = None
    HMS = None

@dataclass
class HiveMetastore(DependencyAwareMixin):
    """
    Configuration for Hive Metastore Service.
    """
    REQUIRED_DEPENDENCIES = ["thrift"]
    
    host: str = "localhost"
    port: int = 9083
    default_db: str = "default"
    auth_mechanism: str = "PLAIN"
    timeout_ms: int = 60000

    def open(self) -> 'HiveClient':
        return HiveClient(self)

class HiveClient(DependencyAwareMixin):
    """
    Runtime client for Hive Metastore.
    """
    REQUIRED_DEPENDENCIES = ["thrift"]

    def __init__(self, config: HiveMetastore):
        self.config = config
        self._transport = None
        self._client = None
    
    def __enter__(self):
        self._connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _connect(self):
        if self._client:
            return

        if HMSvc is None:
            raise ImportError("Hive Metastore Thrift bindings not found.")
            
        socket = TSocket.TSocket(self.config.host, self.config.port)
        socket.setTimeout(self.config.timeout_ms)
        self._transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(self._transport)
        self._client = HMSvc.Client(protocol)
        self._transport.open()
        # print(f"[HiveClient] Connected to {self.config.host}:{self.config.port}")

    def close(self):
        if self._transport:
            self._transport.close()
            self._transport = None
            self._client = None

    def register_delta_table(
        self,
        db_name: str,
        table_name: str,
        location: str,
        schema: pa.Schema,
        partition_keys: List[str] = [],
        table_properties: Optional[Dict[str, str]] = None
    ):
        """
        Registers an external Delta Lake table in Hive.
        """
        self._connect()
        
        # Ensure DB exists
        try:
            self._client.get_database(db_name)
        except HMS.NoSuchObjectException:
            self._create_database(db_name)
            
        # Check if table exists
        try:
            self._client.get_table(db_name, table_name)
            # Table exists, maybe update schema? For now, skip.
            # print(f"[HiveClient] Table {db_name}.{table_name} exists.")
            return
        except HMS.NoSuchObjectException:
            pass
            
        # Register
        self._create_table(db_name, table_name, location, schema, partition_keys, table_properties)

    def _create_database(self, db_name: str):
        db = HMS.Database(
            name=db_name,
            description="Created by ETL Framework",
            locationUri=None,
            parameters={},
            ownerName="hadoop",
            ownerType=HMS.PrincipalType.USER
        )
        self._client.create_database(db)

    def _create_table(
        self, 
        db_name: str, 
        table_name: str, 
        location: str, 
        schema: pa.Schema, 
        partition_keys: List[str], 
        props: Optional[Dict]
    ):
        # Normalize s3 -> s3a
        if location.startswith("s3://"):
            location = location.replace("s3://", "s3a://", 1)
            
        # Schema conversion
        hive_cols = self._arrow_to_hive_columns(schema)
        partition_set = set(partition_keys)
        
        cols = [HMS.FieldSchema(n, t, "") for n, t in hive_cols if n not in partition_set]
        parts = [HMS.FieldSchema(n, t, "") for n, t in hive_cols if n in partition_set]
        
        # Delta SerDe
        serde = HMS.SerDeInfo(
            name="delta-serde",
            serializationLib="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            parameters={}
        )
        
        sd = HMS.StorageDescriptor(
            cols=cols,
            location=location,
            inputFormat="org.apache.hadoop.mapred.SequenceFileInputFormat",
            outputFormat="org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
            serdeInfo=serde,
            parameters={}
        )
        
        params = {
            "EXTERNAL": "TRUE",
            "spark.sql.sources.provider": "delta",
            **(props or {})
        }
        
        table = HMS.Table(
            dbName=db_name,
            tableName=table_name,
            owner="hadoop",
            ownerType=HMS.PrincipalType.USER,
            sd=sd,
            partitionKeys=parts,
            parameters=params,
            tableType="EXTERNAL_TABLE"
        )
        table.storageHandler = "io.delta.hive.DeltaStorageHandler"
        
        self._client.create_table(table)
        print(f"[HiveClient] Registered {db_name}.{table_name}")

    def _arrow_to_hive_columns(self, schema: pa.Schema) -> List[Tuple[str, str]]:
        cols = []
        for f in schema:
            t = f.type
            ht = "string"
            if pa.types.is_int8(t) or pa.types.is_int16(t) or pa.types.is_int32(t): ht = "int"
            elif pa.types.is_int64(t): ht = "bigint"
            elif pa.types.is_float32(t): ht = "float"
            elif pa.types.is_float64(t): ht = "double"
            elif pa.types.is_boolean(t): ht = "boolean"
            elif pa.types.is_timestamp(t): ht = "timestamp"
            elif pa.types.is_date32(t) or pa.types.is_date64(t): ht = "date"
            elif pa.types.is_binary(t): ht = "binary"
            cols.append((f.name, ht))
        return cols
