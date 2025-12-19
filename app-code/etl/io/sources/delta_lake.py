from dataclasses import dataclass, field
from typing import Optional, Dict, Iterator, List, Any
from deltalake import DeltaTable

from etl.io.base import DataSource, DataReader

@dataclass
class DeltaLakeSource(DataSource):
    """
    Configuration for reading from Delta Lake.
    """
    REQUIRED_DEPENDENCIES = ["deltalake", "pandas"]

    uri: str
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    endpoint_url: Optional[str] = None
    region: str = "us-east-1"
    storage_options_extra: Dict[str, str] = field(default_factory=dict)
    
    def open(self) -> 'DeltaLakeReader':
        return DeltaLakeReader(self)

class DeltaLakeReader(DataReader):
    """
    Runtime reader for Delta Lake.
    """
    REQUIRED_DEPENDENCIES = ["deltalake", "pandas"]

    def __init__(self, source: DeltaLakeSource):
        self.source = source
        self._storage_options = self._build_storage_options()

    def _build_storage_options(self) -> Dict[str, str]:
        opts = {
            "AWS_ACCESS_KEY_ID": self.source.aws_access_key_id,
            "AWS_SECRET_ACCESS_KEY": self.source.aws_secret_access_key,
            "AWS_REGION": self.source.region,
        }
        if self.source.endpoint_url:
            opts["AWS_ENDPOINT_URL"] = self.source.endpoint_url
            opts["AWS_S3_FORCE_PATH_STYLE"] = "true"
            
        opts.update(self.source.storage_options_extra)
        return opts

    def read_batch(self) -> Iterator[List[Dict[str, Any]]]:
        """
        Yields the entire table as one batch (for now).
        TODO: Implement batch scanning with PyArrow for large tables.
        """
        try:
            dt = DeltaTable(self.source.uri, storage_options=self._storage_options)
            df = dt.to_pandas()
            # Convert to list of dicts for consistency with other sources
            yield df.to_dict(orient="records")
        except Exception as e:
            print(f"[DeltaLakeReader] Error reading {self.source.uri}: {e}")
            raise

    def close(self):
        pass
