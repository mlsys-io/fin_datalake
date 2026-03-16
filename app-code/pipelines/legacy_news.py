"""
Legacy News Pipeline

Reads News data (JSON metadata + HTML content), joins them, and writes to Delta Lake.
Imports are inside task methods for remote Ray execution.
"""
from typing import Dict, Any
from prefect import flow
from prefect_ray.task_runners import RayTaskRunner

# Only import lightweight base class at module level
from etl.core.base_task import BaseTask

# Load config (auto-loads .env via python-dotenv)
from etl.config import config


class NewsProcessTask(BaseTask):
    """
    Reads News data (JSON metadata + HTML content), joins them, and writes to Delta.
    """
    REQUIRED_DEPENDENCIES = ["ray", "pyarrow", "pandas"]
    
    def run(self, json_pattern: str, html_pattern: str, output_uri: str, hive_config: Dict[str, Any] = None):
        # Heavy imports inside run() - executes on Ray worker
        import glob
        import pyarrow as pa
        from etl.io.sources.file import FileSource
        from etl.io.sinks.delta_lake import DeltaLakeSink
        from etl.services.hive import HiveMetastore
        from etl.config import config
        MAX_CONCURRENCY = config.MAX_CONCURRENCY
        from transformations.news import transform_news
        
        print(f"[{self.name}] Finding files...")
        json_files = glob.glob(json_pattern, recursive=True)
        html_files = glob.glob(html_pattern, recursive=True)
        
        if not json_files:
            print(f"[{self.name}] No JSON files found matching {json_pattern}")
            return
        if not html_files:
            print(f"[{self.name}] No HTML files found matching {html_pattern}")
            return

        print(f"[{self.name}] Found {len(json_files)} JSON files and {len(html_files)} HTML files.")

        # 1. Define Sources
        news_schema = pa.schema([
            pa.field("category", pa.string()),
            pa.field("datetime", pa.int64()),
            pa.field("headline", pa.string()),
            pa.field("id", pa.int64()),
            pa.field("image", pa.string()),
            pa.field("related", pa.string()),
            pa.field("source", pa.string()),
            pa.field("summary", pa.string()),
            pa.field("url", pa.string()),
        ])

        json_source = FileSource(
            paths=json_files,
            format="json",
            ray_read_options={
                "include_paths": True,
                "concurrency": MAX_CONCURRENCY,
                "parse_options": pa.json.ParseOptions(explicit_schema=news_schema)
            }
        )
        
        html_source = FileSource(
            paths=html_files,
            format="text",
            ray_read_options={
                "include_paths": True,
                "concurrency": MAX_CONCURRENCY
            }
        )

        # 2. Read & Transform
        with json_source.open() as json_reader, html_source.open() as html_reader:
            json_ds = next(json_reader.read_batch())
            html_ds = next(html_reader.read_batch())
            
            print(f"[{self.name}] Transforming and Joining Datasets...")
            final_ds = transform_news(json_ds, html_ds)
            
            # 3. Write
            sink = DeltaLakeSink(
                uri=output_uri,
                mode="append",
                hive_config=hive_config,
                hive_table_name="news" if hive_config else None
            )
            
            with sink.open() as writer:
                writer.write_batch(final_ds)
                
        print(f"[{self.name}] Pipeline Complete. Data written to {output_uri}")


@flow(name="Legacy News Pipeline", task_runner=RayTaskRunner(address=config.RAY_ADDRESS))
def news_pipeline(
    input_root: str = "mnt/data/News", 
    output_root: str = "s3://delta-lake/cs4221/bronze-ray",
    hive_host: str = "localhost"
):
    # Paths matching legacy globs
    json_pattern = f"{input_root}/*/news.json"
    html_pattern = f"{input_root}/*/news/*.html"
    output_uri = f"{output_root}/news"
    
    hive_conf = {"host": hive_host}
    
    task_instance = NewsProcessTask(name="News Processor")
    task_instance.as_task().submit(json_pattern, html_pattern, output_uri, hive_conf)


if __name__ == "__main__":
    news_pipeline(
        input_root=os.getenv("INPUT_PATH", "test_data/News"),
        output_root=os.getenv("DELTA_ROOT", "tmp/delta")
    )
