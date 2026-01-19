from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any, Iterator
import ray
import ray.data as rd

from etl.io.base import DataSource, DataReader

@dataclass
class FileSource(DataSource):
    """
    Configuration for reading files from local/S3 filesystem using Ray.
    """
    REQUIRED_DEPENDENCIES = ["ray", "pyarrow"]
    
    paths: List[str]
    format: str = "parquet" # parquet, csv, json, text
    ray_read_options: Dict[str, Any] = field(default_factory=dict)
    
    def open(self) -> 'FileDatasetReader':
        return FileDatasetReader(self)

class FileDatasetReader(DataReader):
    """
    Runtime reader that uses generic ray.data.read_* methods.
    """

    def __init__(self, source: FileSource):
        self.source = source

    def read_batch(self) -> Iterator[rd.Dataset]:
        """
        Yields the entire Ray Dataset as a single 'batch' reference.
        The actual data is lazy.
        """
        fmt = self.source.format.lower()
        paths = self.source.paths
        opts = self.source.ray_read_options

        try:
            ds = None
            if fmt == "parquet":
                ds = rd.read_parquet(paths, **opts)
            elif fmt == "csv":
                ds = rd.read_csv(paths, **opts)
            elif fmt == "json":
                ds = rd.read_json(paths, **opts)
            elif fmt == "text":
                ds = rd.read_text(paths, **opts)
            else:
                raise ValueError(f"Unsupported format: {fmt}")
            
            yield ds
            
        except Exception as e:
            print(f"[FileDatasetReader] Error reading {paths}: {e}")
            raise
    
    def close(self):
        pass
