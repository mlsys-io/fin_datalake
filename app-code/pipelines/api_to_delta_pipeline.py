"""
API to Delta Lake Pipeline

Demonstrates fetching data from a REST API and writing to Delta Lake.
Imports are inside task methods for remote Ray execution.
"""
from typing import List, Dict, Any
from prefect import flow
from prefect_ray.task_runners import RayTaskRunner

# Only import lightweight base class at module level
from etl.core.base_task import BaseTask


# =============================================================================
# 1. Define Ingestion Task (Class-Based)
# =============================================================================

class ApiIngestionTask(BaseTask):
    """
    Fetches data from a REST API using the RestApiSource.
    """
    def run(self, url: str) -> List[Dict[str, Any]]:
        # Heavy imports inside run() - executes on Ray worker
        from etl.io.sources.rest_api import RestApiSource, PaginationConfig
        
        print(f"[{self.name}] Connecting to {url}...")
        
        # Configure Source
        source = RestApiSource(
            url=url,
            pagination=PaginationConfig(type="page", page_param="page"),
            retries=2
        )
        
        all_data = []
        # Runtime Reading
        with source.open() as reader:
            for batch in reader.read_batch():
                print(f"[{self.name}] Fetched batch of {len(batch)} records")
                all_data.extend(batch)
                
        print(f"[{self.name}] Total records fetched: {len(all_data)}")
        return all_data


# =============================================================================
# 2. Define Write Task (Class-Based)
# =============================================================================

class DeltaWriteTask(BaseTask):
    """
    Writes data to a Delta Lake table using the DeltaLakeSink.
    """
    def run(self, data: List[Dict[str, Any]], table_uri: str):
        # Heavy imports inside run() - executes on Ray worker
        import pandas as pd
        from etl.io.sinks.delta_lake import DeltaLakeSink
        
        if not data:
            print(f"[{self.name}] No data to write.")
            return

        print(f"[{self.name}] Writing {len(data)} records to {table_uri}...")
        
        # Convert List[Dict] to DataFrame (Writer expects DF or PA Table)
        df = pd.DataFrame(data)
        
        # Configure Sink
        sink = DeltaLakeSink(
            uri=table_uri,
            mode="append"  # or "overwrite"
        )
        
        # Runtime Writing
        with sink.open() as writer:
            writer.write_batch(df)
            
        print(f"[{self.name}] Write successful.")


# =============================================================================
# 3. Define the Flow
# =============================================================================

@flow(name="API to Delta Pipeline", task_runner=RayTaskRunner)
def api_to_delta_flow(api_url: str, output_path: str):
    
    # Instantiate Tasks Config/Logic
    ingest_logic = ApiIngestionTask(name="Ingest API Data")
    write_logic = DeltaWriteTask(name="Write to Delta")
    
    # Convert to Prefect Tasks
    ingest_task = ingest_logic.as_task(retries=2)
    write_task = write_logic.as_task()
    
    # Execute Pipeline
    raw_data = ingest_task.submit(api_url)
    write_task.submit(raw_data, output_path)


if __name__ == "__main__":
    # Example Local Run
    # Provide a dummy public API for demonstration
    api_to_delta_flow(
        api_url="https://jsonplaceholder.typicode.com/posts",
        output_path="tmp/delta/posts_table"
    )
