"""
Legacy OHLC Pipeline

Reads OHLC data from files, applies legacy transformation, and writes to Delta Lake.
Imports are inside task methods for remote Ray execution.
"""
import os
from typing import Dict, Any
from prefect import flow
from prefect_ray.task_runners import RayTaskRunner

# Only import lightweight base class at module level
from etl.core.base_task import BaseTask

# Load config (auto-loads .env via python-dotenv)
from etl.config import config


class OhlcProcessTask(BaseTask):
    """
    Reads OHLC data from files, applies legacy transformation, and writes to Delta.
    """
    REQUIRED_DEPENDENCIES = ["ray", "pyarrow", "pandas"]
    
    def run(self, input_pattern: str, output_uri: str, hive_config: Dict[str, Any] = None):
        # Heavy imports inside run() - executes on Ray worker
        import glob
        import pyarrow as pa
        from pyarrow import csv
        from etl.io.sources.file import FileSource
        from etl.io.sinks.delta_lake import DeltaLakeSink
        from etl.services.hive import HiveMetastore
        from transformations.ohlc import transform_ohlc
        
        print(f"[{self.name}] finding files matching {input_pattern}...")
        files = glob.glob(input_pattern, recursive=True)
        if not files:
            print(f"[{self.name}] No files found!")
            return

        print(f"[{self.name}] Found {len(files)} files. Reading with Ray...")
        
        # 1. Configure Source (Matching legacy etl.py schemas)
        source = FileSource(
            paths=files,
            format="csv",
            ray_read_options={
                "include_paths": True,
                "convert_options": csv.ConvertOptions(
                    column_types={
                        "timestamp": pa.int64(),
                        "open": pa.float64(),
                        "high": pa.float64(),
                        "low": pa.float64(),
                        "close": pa.float64(),
                        "volume": pa.float64(),
                    }
                )
            }
        )
        
        # 2. Read & Transform
        with source.open() as reader:
            for ds in reader.read_batch():
                # Apply legacy transformation
                transformed_ds = transform_ohlc(ds)
                
                # 3. Write
                sink = DeltaLakeSink(
                    uri=output_uri,
                    mode="append",
                    hive_config=hive_config,
                    hive_table_name="ohlc" if hive_config else None
                )
                
                with sink.open() as writer:
                    writer.write_batch(transformed_ds)
                    
        print(f"[{self.name}] Pipeline Complete. Data written to {output_uri}")


@flow(name="Legacy OHLC Pipeline", task_runner=RayTaskRunner(address=config.RAY_ADDRESS))
def ohlc_pipeline(
    input_root: str = "mnt/data/OHLC", 
    output_root: str = "s3://delta-lake/cs4221/bronze-ray",
    hive_host: str = "localhost"
):
    # Construct paths
    input_pattern = f"{input_root}/*/*.gz"
    output_uri = f"{output_root}/ohlc"
    
    # Config
    hive_conf = {"host": hive_host}
    
    # Task
    task_instance = OhlcProcessTask(name="OHLC Processor")
    task_instance.submit(input_pattern, output_uri, hive_conf)


if __name__ == "__main__":
    # Example usage
    ohlc_pipeline(
        input_root=os.getenv("INPUT_PATH", "test_data/OHLC"),
        output_root=os.getenv("DELTA_ROOT", "tmp/delta")
    )
