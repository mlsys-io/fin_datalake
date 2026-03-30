"""
Hive Metastore client for registering Delta Lake tables.
Heavy imports are deferred to runtime for Ray worker execution.
"""
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Tuple, TYPE_CHECKING
from etl.core.utils.dependency_aware_mixin import DependencyAwareMixin

# Type hints only - not imported at runtime on local machine
if TYPE_CHECKING:
    import pyarrow as pa


def _get_default_hive_host():
    from etl.config import config
    return config.HIVE_HOST or "localhost"

def _get_default_hive_port():
    from etl.config import config
    return config.HIVE_PORT or 9083

@dataclass
class HiveMetastore(DependencyAwareMixin):
    """
    Configuration for Hive Metastore Service.
    """
    REQUIRED_DEPENDENCIES = ["thrift"]
    
    host: str = field(default_factory=_get_default_hive_host)
    port: int = field(default_factory=_get_default_hive_port)
    default_db: str = "default"
    auth_mechanism: str = "PLAIN"
    timeout_ms: int = 60000

    def open(self) -> 'HiveClient':
        return HiveClient(self)


class HiveClient(DependencyAwareMixin):
    """
    Runtime client for Hive Metastore.
    Heavy imports happen here, not at module level.
    """
    REQUIRED_DEPENDENCIES = ["thrift"]

    def __init__(self, config: HiveMetastore):
        self.config = config
        self._transport = None
        self._client = None
        self._HMSvc = None
        self._HMS = None
    
    def __enter__(self):
        self._connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _load_thrift_modules(self):
        """Lazy load Thrift modules only when needed."""
        if self._HMSvc is not None:
            return
            
        try:
            from thrift.transport import TSocket, TTransport
            from thrift.protocol import TBinaryProtocol
            from etl.vendor.hms.hive_metastore import ThriftHiveMetastore as HMSvc
            from etl.vendor.hms.hive_metastore import ttypes as HMS
            
            self._TSocket = TSocket
            self._TTransport = TTransport
            self._TBinaryProtocol = TBinaryProtocol
            self._HMSvc = HMSvc
            self._HMS = HMS
        except ImportError as e:
            raise ImportError(
                f"Hive Metastore requires 'thrift' and generated Thrift bindings. "
                f"Error: {e}"
            )

    def _connect(self):
        if self._client:
            return

        self._load_thrift_modules()
            
        socket = self._TSocket.TSocket(self.config.host, self.config.port)
        socket.setTimeout(self.config.timeout_ms)
        self._transport = self._TTransport.TBufferedTransport(socket)
        protocol = self._TBinaryProtocol.TBinaryProtocol(self._transport)
        self._client = self._HMSvc.Client(protocol)
        self._transport.open()

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
        schema: Any,  # pyarrow.Schema
        partition_keys: Optional[List[str]] = None,
        table_properties: Optional[Dict[str, str]] = None
    ):
        """
        Registers an external Delta Lake table in Hive.
        """
        partition_keys = partition_keys or []
        self._connect()
        HMS = self._HMS
        
        # Ensure DB exists
        try:
            self._client.get_database(db_name)
        except HMS.NoSuchObjectException:
            self._create_database(db_name)
            
        # Check if table exists
        try:
            self._client.get_table(db_name, table_name)
            return  # Table exists
        except HMS.NoSuchObjectException:
            pass
            
        # Register
        self._create_table(db_name, table_name, location, schema, partition_keys, table_properties)

    def get_all_tables(self, db_name: str = "default") -> List[Dict[str, str]]:
        """
        Returns a list of all tables in a database with their names and S3 paths.
        """
        self._connect()
        try:
            table_names = self._client.get_all_tables(db_name)
            results = []
            for name in table_names:
                try:
                    table = self._client.get_table(db_name, name)
                    # Normalize s3a -> s3 for the Gateway's consumption
                    location = table.sd.location
                    if location.startswith("s3a://"):
                        location = location.replace("s3a://", "s3://", 1)
                    results.append({"name": name, "path": location})
                except Exception as e:
                    print(f"[HiveClient] Failed to get metadata for table {name}: {e}")
            return results
        except Exception as e:
            print(f"[HiveClient] Failed to list tables in db {db_name}: {e}")
            raise

    def get_table(self, db_name: str, table_name: str) -> Any:
        """
        Returns the raw Hive Table object.
        """
        self._connect()
        return self._client.get_table(db_name, table_name)

    def _create_database(self, db_name: str):
        HMS = self._HMS
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
        schema: Any,  # pyarrow.Schema
        partition_keys: List[str], 
        props: Optional[Dict]
    ):
        HMS = self._HMS
        
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

    def _arrow_to_hive_columns(self, schema: Any) -> List[Tuple[str, str]]:
        """Convert PyArrow schema to Hive column definitions."""
        import pyarrow as pa
        
        cols = []
        for f in schema:
            t = f.type
            ht = "string"
            if pa.types.is_int8(t) or pa.types.is_int16(t) or pa.types.is_int32(t): 
                ht = "int"
            elif pa.types.is_int64(t): 
                ht = "bigint"
            elif pa.types.is_float32(t): 
                ht = "float"
            elif pa.types.is_float64(t): 
                ht = "double"
            elif pa.types.is_boolean(t): 
                ht = "boolean"
            elif pa.types.is_timestamp(t): 
                ht = "timestamp"
            elif pa.types.is_date32(t) or pa.types.is_date64(t): 
                ht = "date"
            elif pa.types.is_binary(t): 
                ht = "binary"
            cols.append((f.name, ht))
        return cols
