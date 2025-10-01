from contextlib import contextmanager
import os
from typing import List, Optional, Sequence, Tuple
import pyarrow as pa

from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol

from ioutils.hms_client.hive_metastore import ThriftHiveMetastore as HMSvc
from ioutils.hms_client.hive_metastore import ttypes as HMS

HMS_DB = os.getenv("HMS_DB", "default")
HMS_HOST = os.getenv("HMS_HOST", "localhost")
HMS_PORT = int(os.getenv("HMS_PORT", "9083"))

class HiveMetastoreClient:

    def __init__(self, host: str = HMS_HOST, port: int = HMS_PORT, timeout_ms: int = 60000):
        self.host = host
        self.port = port
        self.sock = TSocket.TSocket(self.host, self.port)
        self.sock.setTimeout(timeout_ms)
        self.trans = TTransport.TBufferedTransport(self.sock)
        self.proto = TBinaryProtocol.TBinaryProtocol(self.trans)
        self.client = HMSvc.Client(self.proto)

    # --- lifecycle ------------------------------------------------------------

    def open(self) -> None:
        self.trans.open()

    def close(self) -> None:
        try:
            self.trans.close()
        except Exception:
            pass

    def __enter__(self) -> "HiveMetastoreClient":
        self.open()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # --- database ops ---------------------------------------------------------

    def database_exists(self, db: str) -> bool:
        try:
            self.client.get_database(db)
            return True
        except HMS.NoSuchObjectException:
            return False

    def ensure_database(self, db: str, location_uri: Optional[str] = None, description: str = "") -> None:
        if self.database_exists(db):
            return
        database = HMS.Database(
            name=db,
            description=description,
            locationUri=location_uri,
            parameters={},
            ownerName="owner",
            ownerType=HMS.PrincipalType.USER,
        )
        self.client.create_database(database)

    # --- table ops ------------------------------------------------------------

    def get_table(self, db: str, table: str) -> HMS.GetTableResult:
        if hasattr(self.client, "get_table"):
            return self.client.get_table(db, table)

        if hasattr(self.client, "get_table_req"):
            req = HMS.GetTableRequest(dbName=db, tblName=table)
            return self.client.get_table_req(req)

        raise NotImplementedError("This HMS client has no get_table* method.")

    def table_exists(self, db: str, table: str) -> bool:
        try:
            self.get_table(db, table)
            return True
        except HMS.NoSuchObjectException:
            return False

    def get_table_location(self, db: str, table: str) -> str:
        tbl: HMS.Table = self.get_table(db, table).table
        return tbl.sd.location

    def drop_table_if_exists(self, db: str, table: str, delete_data: bool = False) -> None:
        if self.table_exists(db, table):
            self.client.drop_table(db, table, deleteData=delete_data)

    # --- delta registration ----------------------------------------------------

    def ensure_delta_table(
        self,
        db: str,
        table: str,
        location: str,
        columns: Sequence[Tuple[str, str]],
        partition_keys: Sequence[Tuple[str, str]] = (),
        table_properties: Optional[dict] = None,
    ) -> None:
        """
        Register (or verify) an EXTERNAL Delta table in HMS using Delta's storage handler.
        NOTE: Hive with delta-hive is read-only.

        :param columns: list of (name, hive_type) for data columns.
                        Use Hive types like: string, bigint, double, boolean, int, float, decimal(18,2), timestamp, date
        :param partition_keys: list of (name, hive_type) for partition columns (if any).
        :param table_properties: extra table parameters (e.g. {"provider": "delta"})
        """
        if self.table_exists(db, table):
            # If it exists, we’re done (you could add verification/patching here if desired)
            return

        # Convert to FieldSchema list
        def fs(cols: Sequence[Tuple[str, str]]) -> List[HMS.FieldSchema]:
            return [HMS.FieldSchema(name=c[0], type=c[1], comment="") for c in cols]

        serde = HMS.SerDeInfo(
            name="delta-serde",
            serializationLib="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            parameters={},
        )

        sd = HMS.StorageDescriptor(
            cols=fs(columns),
            location=location,
            inputFormat=None,   # With storage handler, these can be None
            outputFormat=None,
            compressed=False,
            numBuckets=-1,
            serdeInfo=serde,
            bucketCols=[],
            sortCols=[],
            parameters={},
            skewedInfo=None,
            storedAsSubDirectories=False,
        )

        tbl = HMS.Table(
            dbName=db,
            tableName=table,
            owner="owner",
            sd=sd,
            partitionKeys=fs(partition_keys),
            parameters={
                "EXTERNAL": "TRUE",
                "provider": "delta",  # helpful hint for other engines
                **(table_properties or {}),
            },
            tableType="EXTERNAL_TABLE",
        )
        # Storage handler is the key bit for delta-hive:
        tbl.storageHandler = "io.delta.hive.DeltaStorageHandler"

        self.client.create_table(tbl)

    # --- helpers --------------------------------------------------------------

    @staticmethod
    def hive_columns_from_arrow(arrow_schema: pa.Schema) -> List[Tuple[str, str]]:
        """
        Best-effort mapping from a pyarrow.Schema to Hive column types.
        You can tweak to your needs.
        """
        import pyarrow as pa

        def to_hive_type(t: pa.DataType) -> str:
            if pa.types.is_int8(t) or pa.types.is_int16(t) or pa.types.is_int32(t):
                return "int"
            if pa.types.is_int64(t):
                return "bigint"
            if pa.types.is_float32(t):
                return "float"
            if pa.types.is_float64(t):
                return "double"
            if pa.types.is_boolean(t):
                return "boolean"
            if pa.types.is_timestamp(t):
                return "timestamp"
            if pa.types.is_date32(t) or pa.types.is_date64(t):
                return "date"
            if pa.types.is_decimal(t):
                return f"decimal({t.precision},{t.scale})"
            # default
            return "string"

        return [(name, to_hive_type(type)) for name, type in zip(arrow_schema.names, arrow_schema.types)]

@contextmanager
def open_hms(host: str = "localhost", port: int = 9083):
    """One-shot context manager."""
    client = HiveMetastoreClient(host, port)
    try:
        client.open()
        yield client
    finally:
        client.close()

def register_delta_table_in_hive(
    db: str, 
    table: str, 
    location: str, 
    columns: Sequence[Tuple[str, str]],
    partition_keys: list[str] = []):
    if "s3://" in location:
        location = location.replace("s3://", "s3a://", 1)
    with open_hms(HMS_HOST, HMS_PORT) as hms:
        hms.ensure_database(db)
        hms.ensure_delta_table(
            db=db,
            table=table,
            location=location,
            columns=columns,
            partition_keys=partition_keys, 
            table_properties={"provider": "delta"},
        )

def get_hive_table_location(db: str, table: str) -> str:
    with open_hms(HMS_HOST, HMS_PORT) as hms:
        return hms.get_table_location(db, table)