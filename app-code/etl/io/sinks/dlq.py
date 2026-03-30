import uuid
from datetime import datetime, timezone
from loguru import logger
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa

class DeadLetterQueue:
    """
    Routes failed micro-batches (e.g. schema mismatches) to a safe S3 location
    instead of crashing the entire pipeline.
    """
    def __init__(self, base_uri: str, storage_options: dict = None):
        if not base_uri.endswith('/'):
            base_uri += '/'
        self.base_uri = base_uri
        self.storage_options = storage_options or {}

    def send(self, data: pa.Table, error_message: str):
        try:
            from pyarrow import parquet as pq
            import pyarrow.fs as fs
            import urllib.parse
            
            # Construct S3/local file path
            file_name = f"{datetime.now(timezone.utc).strftime('%Y-%m-%d')}/{uuid.uuid4()}.parquet"
            full_path = self.base_uri + file_name
            
            logger.info(f"[DLQ] Routing failed batch to DLQ: {full_path}")
            
            # Setup S3 connection if AWS keys are provided
            filesystem = None
            if full_path.startswith("s3://"):
                parsed = urllib.parse.urlparse(full_path)
                bucket, path = parsed.netloc, parsed.path.lstrip('/')
                target = f"{bucket}/{path}"
                
                s3_opts = {
                    "access_key": self.storage_options.get("AWS_ACCESS_KEY_ID"),
                    "secret_key": self.storage_options.get("AWS_SECRET_ACCESS_KEY"),
                    "region": self.storage_options.get("AWS_REGION", "us-east-1"),
                }
                
                if "AWS_ENDPOINT_URL" in self.storage_options:
                    # Strip http(s):// for pyarrow fs
                    ep = self.storage_options["AWS_ENDPOINT_URL"].replace("http://", "").replace("https://", "")
                    s3_opts["endpoint_override"] = ep
                    s3_opts["scheme"] = "http" if "http://" in self.storage_options["AWS_ENDPOINT_URL"] else "https"
                    
                s3_opts = {k: v for k, v in s3_opts.items() if v}
                filesystem = fs.S3FileSystem(**s3_opts)
            else:
                target = full_path
                filesystem = fs.LocalFileSystem()

            # Attach error metadata so engineers can debug
            metadata = data.schema.metadata or {}
            metadata[b"dlq_error"] = str(error_message).encode('utf-8')
            data = data.replace_schema_metadata(metadata)
            
            pq.write_table(data, target, filesystem=filesystem)
            
            logger.success(f"[DLQ] Successfully captured {data.num_rows} rows.")
        except Exception as e:
            logger.error(f"[DLQ] FATAL: Failed to write to Dead Letter Queue: {e}")
