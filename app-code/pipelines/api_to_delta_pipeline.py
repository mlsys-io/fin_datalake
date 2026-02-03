"""
API to Delta Lake Pipeline

Demonstrates fetching data from a REST API and writing to Delta Lake.
Uses BaseTask abstraction with imports inside run() for Ray execution.
"""
from typing import List, Dict, Any
from prefect import flow
from prefect_ray.task_runners import RayTaskRunner

# Only import lightweight base class at module level
from etl.core.base_task import BaseTask

# Load config (auto-loads .env via python-dotenv)
from etl.config import config


# =============================================================================
# 1. Define Ingestion Task (Class-Based)
# =============================================================================

class ApiIngestionTask(BaseTask):
    """
    Fetches data from a REST API using requests.
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
# 2. Define Write Task (Class-Based)
# =============================================================================

class DeltaWriteTask(BaseTask):
    """
    Writes data to a Delta Lake table.
    """
    def run(self, data: List[Dict[str, Any]], table_uri: str) -> str:
        # Heavy imports inside run() - executes on Ray worker
        import pandas as pd
        from deltalake import write_deltalake
        from etl.config import config  # Import inside run for Ray worker
        
        if not data:
            print(f"[{self.name}] No data to write.")
            return "No data"

        print(f"[{self.name}] Writing {len(data)} records to {table_uri}...")
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # S3 storage options from config (MinIO compatible)
        storage_options = None
        if table_uri.startswith("s3://"):
            storage_options = {
                "AWS_ACCESS_KEY_ID": config.AWS_ACCESS_KEY_ID,
                "AWS_SECRET_ACCESS_KEY": config.AWS_SECRET_ACCESS_KEY,
                "AWS_ENDPOINT_URL": config.AWS_ENDPOINT_URL,
                "AWS_REGION": config.AWS_REGION,
                "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
                # Allow HTTP for local MinIO (if not using HTTPS)
                "AWS_ALLOW_HTTP": "false",
            }
            print(f"[{self.name}] Using S3 endpoint: {config.AWS_ENDPOINT_URL}")
        
        write_deltalake(
            table_uri, 
            df, 
            mode="overwrite",
            storage_options=storage_options
        )
            
        print(f"[{self.name}] Successfully wrote {len(data)} records")
        return f"Wrote {len(data)} records to {table_uri}"


# =============================================================================
# 3. Define the Flow
# =============================================================================

@flow(name="API to Delta Pipeline", task_runner=RayTaskRunner(address=config.RAY_ADDRESS))
def api_to_delta_flow(
    api_url: str = "https://jsonplaceholder.typicode.com/posts",
    output_path: str = "s3://delta-lake/bronze/api_posts"
):
    """
    Pipeline that fetches data from an API and writes to Delta Lake.
    """
    print(f"[flow] Starting API to Delta Pipeline")
    print(f"[flow] Ray Address: {config.RAY_ADDRESS}")
    
    # Instantiate Task Logic
    ingest_logic = ApiIngestionTask(name="Fetch API Data")
    write_logic = DeltaWriteTask(name="Write to Delta")
    
    # Convert to Prefect Tasks
    ingest_task = ingest_logic.as_task(retries=2)
    write_task = write_logic.as_task()
    
    # Execute Pipeline
    raw_data = ingest_task.submit(api_url)
    result = write_task.submit(raw_data, output_path)
    
    # Get result
    final = result.result()
    print(f"[flow] Pipeline complete: {final}")
    return final


if __name__ == "__main__":
    api_to_delta_flow()
