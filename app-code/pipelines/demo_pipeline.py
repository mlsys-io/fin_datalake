"""
Demo Pipeline

Example pipeline showing class-based and functional task patterns.
Imports are inside task methods for remote Ray execution.
"""
import os
from typing import List, Dict, Any
from prefect import flow, task
from prefect_ray.task_runners import RayTaskRunner

# Only import lightweight base class at module level
from etl.core.base_task import BaseTask

# Read Ray cluster address from environment
RAY_ADDRESS = os.environ.get("RAY_ADDRESS", "auto")


class DataFilteringTask(BaseTask):
    """
    Example of a Class-based Task using the new pattern.
    """
    def run(self, data: List[Dict]) -> List[Dict]:
        min_id = self.config.get("min_id", 0)
        print(f"[DataFilteringTask] Filtering items with id < {min_id}")
        return [d for d in data if d.get("id", 0) >= min_id]


@task(retries=2)
def ingest_data(url: str, source_type: str = "rest_api") -> List[Dict]:
    """
    Wraps the specific Ingestion Connector logic in a Prefect Task.
    Executes remotely if RayTaskRunner is used.
    """
    # Heavy imports inside task - executes on Ray worker
    from etl.io.sources.rest_api import RestApiSource, PaginationConfig
    
    print(f"Ingesting from {url}...")
    
    # Define Source (Serializable)
    source = RestApiSource(
        url=url,
        pagination=PaginationConfig(type="page", page_param="page")
    )
    
    # Open Reader (Runtime)
    all_data = []
    with source.open() as reader:
        for batch in reader.read_batch():
            all_data.extend(batch)
            
    return all_data


@task
def transform_data(data: List[Dict]) -> List[Dict]:
    """Simple transformation task."""
    print(f"Transforming {len(data)} records...")
    return [d for d in data if d.get("id")]  # Simple filter


@flow(task_runner=RayTaskRunner(address=RAY_ADDRESS))
def main_pipeline(api_url: str):
    """
    Demo pipeline showing hybrid task patterns.
    Using RayTaskRunner - all tasks execute on Ray cluster.
    """
    # 1. Functional Task
    raw_data_future = ingest_data.submit(api_url)
    
    # 2. Class-Based Task (The new Pattern)
    # Define/Instantiate logic
    filter_logic = DataFilteringTask(config={"min_id": 5})
    # Convert to Prefect Task
    filter_task = filter_logic.as_task(retries=1)
    
    filtered_future = filter_task.submit(raw_data_future)
    
    # 3. Functional Transform
    processed_data = transform_data.submit(filtered_future)
    
    final_result = processed_data.result()
    print(f"Pipeline finished with {len(final_result)} records.")
    return final_result


if __name__ == "__main__":
    # Local test run
    main_pipeline(api_url="https://jsonplaceholder.typicode.com/posts")

