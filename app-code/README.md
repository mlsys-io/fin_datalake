# ETL Framework

A distributed ETL framework built on Prefect and Ray for scalable data pipelines.

## Installation

### Client Side (Local Machine)

```bash
cd app-code
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-client.txt
```

### Ray Workers (Kubernetes)

Workers use a custom Docker image with all dependencies pre-installed.
See `deps/kuberay/Dockerfile` for the worker image.

---

## ⚠️ Important: Import Pattern for Ray Execution

Since pipelines run on **Ray workers** (not your local machine), heavy dependencies must be imported **inside task functions**, not at module level.

### ❌ WRONG - Will fail on client

```python
from etl.io.sinks.delta_lake import DeltaLakeSink  # Imports pyarrow, deltalake
from etl.agents import BaseAgent  # Imports ray

@task
def my_task():
    sink = DeltaLakeSink(...)  # Already imported at module level!
```

### ✅ CORRECT - Import inside task

```python
from prefect import flow, task

@task
def write_to_delta(data: list, uri: str):
    # Import INSIDE the task - runs on Ray worker
    from etl.io.sinks.delta_lake import DeltaLakeSink
    
    sink = DeltaLakeSink(uri=uri, ...)
    with sink.open() as writer:
        writer.write_batch(data)

@flow
def my_pipeline():
    write_to_delta(data, "s3://bucket/table")
```

### Summary

| Import Location | When Executed | Works on Client? |
|-----------------|---------------|------------------|
| Module level (top of file) | On `import` | ❌ No (missing deps) |
| Inside `@task` function | On Ray worker | ✅ Yes |
| Inside `@flow` function | On client | ❌ No |

---

## Module Overview

### `etl.core`
- `BaseTask` - Base class for ETL tasks
- `ServiceTask` - Base class for long-running services (Ray Actors)

### `etl.io.sources`
- `RestApiSource` - REST API consumption with pagination
- `KafkaSource` - Kafka topic consumption
- `WebSocketSource` - WebSocket streaming
- `DeltaLakeSource` - Read from Delta Lake tables

### `etl.io.sinks`
- `DeltaLakeSink` - Write to Delta Lake with Hive registration
- `TimescaleDBSink` - Write to TimescaleDB
- `MilvusSink` - Write to Milvus vector database
- `HttpSink` - Send to webhooks/APIs

### `etl.agents`
- `BaseAgent` - Base class for AI agents (Ray Actor)
- `AgentRegistry` - Service discovery for agents
- `MessageBus` - Pub/sub messaging between agents
- `ContextStore` - Shared state for agent coordination

---

## Running Pipelines

```bash
# Activate environment
cd ~/zdb_deployment/app-code
source .venv/bin/activate

# Load environment variables
source ~/zdb_deployment/.env

# Run a pipeline
python -m pipelines.demo_pipeline
```
