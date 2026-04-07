"""
Legacy SEC Pipeline

Reads SEC Filings (Text), processes them, and writes to Delta Lake.
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


class SecProcessTask(BaseTask):
    """
    Reads SEC Filings (Text), processes them, and writes to Delta.
    """
    REQUIRED_DEPENDENCIES = ["ray", "pyarrow", "pandas"]
    
    def run(self, input_pattern: str, output_uri: str, hive_config: Dict[str, Any] = None):
        # Heavy imports inside run() - executes on Ray worker
        import glob
        from etl.io.sources.file import FileSource
        from etl.io.sinks.delta_lake import DeltaLakeSink
        from etl.services.hive import HiveMetastore
        from etl.config import config
        MAX_CONCURRENCY = config.MAX_CONCURRENCY
        from transformations.sec_filings import transform_sec
        
        print(f"[{self.name}] Finding files matching {input_pattern}...")
        files = glob.glob(input_pattern, recursive=True)
        
        if not files:
            print(f"[{self.name}] No SEC files found!")
            return

        print(f"[{self.name}] Found {len(files)} files.")

        # 1. Define Source
        source = FileSource(
            paths=files,
            format="text",
            ray_read_options={
                "include_paths": True,
                "concurrency": MAX_CONCURRENCY
            }
        )

        # 2. Read & Transform
        with source.open() as reader:
            ds = next(reader.read_batch())
            
            print(f"[{self.name}] Transforming SEC Data...")
            transformed_ds = transform_sec(ds)
            
            # 3. Write
            sink = DeltaLakeSink(
                uri=output_uri,
                mode="append",
                hive_config=hive_config,
                hive_table_name="sec_filings" if hive_config else None
            )
            
            with sink.open() as writer:
                writer.write_batch(transformed_ds)
                
        print(f"[{self.name}] Pipeline Complete. Data written to {output_uri}")


@flow(name="Legacy SEC Pipeline", task_runner=RayTaskRunner(address=config.RAY_ADDRESS))
def sec_pipeline(
    input_root: str = "mnt/data/SEC-Filings", 
    output_root: str = "s3://delta-lake/cs4221/bronze-ray",
    hive_host: str = "localhost"
):
    # Match legacy glob: {INPUT_PATH}/SEC-Filings/*/10-Q/*/full-submission.txt
    input_pattern = f"{input_root}/*/10-Q/*/full-submission.txt"
    output_uri = f"{output_root}/sec_filings"
    
    hive_conf = {"host": hive_host}
    
    task_instance = SecProcessTask(name="SEC Processor")
    task_instance.submit(input_pattern, output_uri, hive_conf)


if __name__ == "__main__":
    sec_pipeline(
        input_root=os.getenv("INPUT_PATH", "test_data/SEC-Filings"),
        output_root=os.getenv("DELTA_ROOT", "tmp/delta")
    )
