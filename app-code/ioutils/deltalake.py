import os
import numpy as np
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable

import ray.data as rd

from loguru import logger

MINIO_USERNAME = os.getenv("MINIO_USERNAME")
MINIO_PASSWORD = os.getenv("MINIO_PASSWORD")
MINIO_SERVER_URL = os.getenv("MINIO_SERVER_URL")
CA_PATH = os.getenv("CA_PATH", "/opt/certs/public.crt")
INPUT_PATH = os.getenv("INPUT_PATH", "/mnt/data")
DELTA_ROOT = os.getenv("DELTA_ROOT", "s3://delta-lake/cs4221/bronze-ray")
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "10"))
os.environ.setdefault("RUST_LOG", "warn")
os.environ.setdefault("SSL_CERT_FILE", CA_PATH)
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")

def storage_opts_for_delta() -> dict:
    opts = {
        "aws_endpoint": MINIO_SERVER_URL,
        "aws_access_key_id": MINIO_USERNAME,
        "aws_secret_access_key": MINIO_PASSWORD,
        "conditional_put": "etag", # https://delta-io.github.io/delta-rs/usage/writing/writing-to-s3-with-locking-provider
        "aws_s3_force_path_style": "true"
    }
    return opts

def _strip_tz_from_schema(schema: pa.Schema) -> pa.Schema:
    new_fields = []
    for f in schema:
        t = f.type
        if pa.types.is_timestamp(t) and getattr(t, "tz", None):
            t = pa.timestamp(t.unit)  # same unit, no tz
        new_fields.append(pa.field(f.name, t, f.nullable, f.metadata))
    return pa.schema(new_fields)

OVERWRITE_SCHEMA_TYPES = {
    "_source_path": pa.string(),
    "symbol": pa.string(),
}
def arrow_schema_from_dataset(ds: rd.Dataset) -> pa.schema:
    schema = ds.schema()
    if isinstance(schema.base_schema, rd.dataset.PandasBlockSchema):
        schema.base_schema = rd.dataset.PandasBlockSchema(
            names=schema.base_schema.names,
            types=[
                np.str_ if isinstance(t, pd.core.arrays.string_.StringDtype) else t
                for t in schema.base_schema.types
            ] # Suppress the error `Cannot interpret 'string[python]' as a data type`
        )
    if not all(isinstance(t, pa.DataType) for t in schema.types):
        logger.warning(f"Detecting unexpected pattern in schema with string: {schema}")
    # HACK: overwrite some metadata columns to string. They are considered objects/None by ray data because ds.Schema infer arrow dtype from numpy (https://github.com/ray-project/ray/blob/f8572754424b5be34593a903e88ac725ba171c7d/python/ray/data/dataset.py#L6420). We should find a way to bypass this problem. 
    # TODO: Avoid string to be set as object, and use binary for real objects (images, pdfs, etc)
    types = [OVERWRITE_SCHEMA_TYPES.get(n, t) for n, t in zip(schema.names, schema.types)]
    if not all(isinstance(t, pa.DataType) for t in types):
        logger.error(f"Detecting unexpected pattern in schema after overwrite: {types}")
    return pa.schema(list(zip(schema.names, types)))

def ensure_delta_table(uri: str, schema: pa.Schema, storage_options: dict, partition_by=None):
    """Create the table once on the driver, otherwise no-op."""
    # Try open; if it exists we're done.
    try:
        DeltaTable(uri, storage_options=storage_options)
        print(f"[delta] Table already exists at {uri}")
        return
    except Exception as e:
        print(f"[delta] Table not found at {uri}, creating… ({e.__class__.__name__})")

    DeltaTable.create(
        table_uri=uri,
        schema=_strip_tz_from_schema(schema),
        mode="ignore",                 # idempotent: do nothing if it appears between check & create
        partition_by=partition_by or [],
        storage_options=storage_options,
    )
    print("[delta] Created table via DeltaTable.create()")

class DeltaLakeSink(rd.Datasink):
    def __init__(self, uri: str, mode: str = "append", storage_options: dict = {}):
        self.uri = uri
        self.mode = mode
        self.storage_options = storage_options

    def write(self, blocks, ctx):
        from deltalake import write_deltalake
        for block in blocks:
            if not isinstance(block, pa.Table):
                if isinstance(block, pd.DataFrame):
                    block = pa.Table.from_pandas(block, preserve_index=False)
                else:
                    block = getattr(block, "to_arrow", lambda: None)() or pa.table(block)
            write_deltalake(self.uri, block, mode=self.mode, storage_options=self.storage_options)

def write_delta_distributed(ds: rd.Dataset, delta_uri: str, *, storage_options: dict, target_files: int = 8):
    arrow_schema = arrow_schema_from_dataset(ds)
    ensure_delta_table(delta_uri, arrow_schema, storage_options, partition_by=[])

    sink = DeltaLakeSink(
        uri=delta_uri,
        mode="append",
        storage_options=storage_options,
    )
    ds.repartition(target_files).write_datasink(sink, concurrency=MAX_CONCURRENCY)