"""
API to Delta Lake Pipeline

Demonstrates fetching data from a REST API and writing to Delta Lake.
Uses the explicit BaseTask submit/local API with DeltaLakeWriteTask for proper conflict handling.
"""
from typing import List, Dict, Any
from prefect import flow
from prefect_ray.task_runners import RayTaskRunner

# Import base class and ready-to-use Delta task
from etl.core.base_task import BaseTask
from etl.io import DeltaLakeWriteTask

# Load config (auto-loads .env via python-dotenv)
from etl.config import config


# =============================================================================
# 1. Define Ingestion Task (Class-Based)
# =============================================================================

class ApiIngestionTask(BaseTask):
    """
    Fetches data from a REST API using requests.
    Safe to run distributed on Ray workers.
    """
    def run(self, url: str) -> List[Dict[str, Any]]:
        # Heavy imports inside run() - executes on Ray worker
        import requests
        
        print(f"[{self.name}] Fetching from {url}...")
        
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
                
        print(f"[{self.name}] Fetched {len(data)} records")
        return data


# =============================================================================
# 2. Define the Flow
# =============================================================================

@flow(name="API to Delta Pipeline", task_runner=RayTaskRunner(address=config.RAY_ADDRESS))
def api_to_delta_flow(
    api_url: str = "https://jsonplaceholder.typicode.com/posts",
    output_path: str = "s3://delta-lake/bronze/api_posts"
):
    """
    Pipeline that fetches data from an API and writes to Delta Lake.
    
    - Ingestion: Runs distributed on Ray workers
    - Delta Write: Runs locally (avoids Tokio/Ray conflict)
    """
    print(f"[flow] Starting API to Delta Pipeline")
    print(f"[flow] Ray Address: {config.RAY_ADDRESS}")
    
    # Create tasks with clean API (retries in constructor)
    ingest_task = ApiIngestionTask(name="Fetch API Data", retries=2)
    write_task = DeltaLakeWriteTask(
        name="Write to Delta",
        uri=output_path,
        mode="overwrite"
    )
    
    # Execute: Ingest on Ray (distributed)
    raw_data = ingest_task.submit(api_url)
    
    # Get data (wait for Ray)
    data = raw_data.result()
    print(f"[flow] Fetched {len(data)} records from API")
    
    # Execute: Write using Ray Data (Distributed)
    # Now uses distributed execution thanks to Ray Data integration
    write_future = write_task.submit(data)
    result = write_future.result()
    
    print(f"[flow] Pipeline complete: {result}")
    return result


if __name__ == "__main__":
    api_to_delta_flow()
