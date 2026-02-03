"""
Demo Pipeline - Self-Contained

A simple demo pipeline that runs entirely on Ray cluster without
needing any custom etl package installed on workers.
"""
from typing import List, Dict
from prefect import flow, task
from prefect_ray.task_runners import RayTaskRunner

# Load config locally (python-dotenv auto-loads .env)
import os
from pathlib import Path
try:
    from dotenv import load_dotenv
    # Find .env file
    for p in [Path.cwd() / ".env", Path.cwd().parent / ".env"]:
        if p.exists():
            load_dotenv(p)
            break
except ImportError:
    pass

RAY_ADDRESS = os.environ.get("RAY_ADDRESS", "auto")


@task(retries=2, name="Fetch Data from API")
def fetch_data(url: str) -> List[Dict]:
    """
    Fetch data from a REST API.
    All imports are inside the task - runs on Ray worker.
    """
    import requests
    
    print(f"[fetch_data] Fetching from {url}...")
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()
    print(f"[fetch_data] Received {len(data)} records")
    return data


@task(name="Filter Data")
def filter_data(data: List[Dict], min_id: int = 5) -> List[Dict]:
    """
    Filter records by ID threshold.
    Pure Python - no external imports needed.
    """
    print(f"[filter_data] Filtering {len(data)} records (min_id={min_id})...")
    filtered = [d for d in data if d.get("id", 0) >= min_id]
    print(f"[filter_data] Kept {len(filtered)} records")
    return filtered


@task(name="Transform Data")
def transform_data(data: List[Dict]) -> List[Dict]:
    """
    Apply transformations to the data.
    Pure Python - no external imports needed.
    """
    print(f"[transform_data] Transforming {len(data)} records...")
    
    # Example transformation: add a computed field
    for record in data:
        record["title_length"] = len(record.get("title", ""))
    
    return data


@flow(name="Demo Pipeline", task_runner=RayTaskRunner(address=RAY_ADDRESS))
def demo_pipeline(api_url: str = "https://jsonplaceholder.typicode.com/posts"):
    """
    Demo pipeline showing distributed execution on Ray.
    
    All tasks run on Ray workers, not locally.
    """
    print(f"[flow] Starting pipeline with Ray at {RAY_ADDRESS}")
    
    # 1. Fetch data (runs on Ray worker)
    raw_data = fetch_data.submit(api_url)
    
    # 2. Filter data (runs on Ray worker)
    filtered_data = filter_data.submit(raw_data, min_id=10)
    
    # 3. Transform data (runs on Ray worker)
    final_data = transform_data.submit(filtered_data)
    
    # Get result
    result = final_data.result()
    print(f"[flow] Pipeline complete! {len(result)} records processed.")
    
    return result


if __name__ == "__main__":
    result = demo_pipeline()
    print(f"\nFirst 3 records:")
    for r in result[:3]:
        print(f"  - ID {r['id']}: {r['title'][:40]}... (len={r['title_length']})")
