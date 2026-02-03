# ETL Framework

A distributed ETL framework built on Prefect and Ray for scalable data pipelines.

## Quick Start

```bash
cd ~/zdb_deployment/app-code
source ~/zdb_deployment/.env
source .venv/bin/activate

# Configure Prefect to use dedicated server
prefect config set PREFECT_API_URL="http://${NODE_IP}:30420/api"

# Run a pipeline
python pipelines/demo_pipeline.py
```

---

## Installation

### 1. Create Virtual Environment

```bash
cd ~/zdb_deployment/app-code
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Install Dependencies

```bash
# Install the ETL framework package
pip install -e .

# Install client dependencies
pip install -r requirements-client.txt

# Install Ray (must match cluster version)
pip install ray
```

### 3. Configure Services

```bash
# Generate environment variables
cd ~/zdb_deployment
bash setup-config.sh
source .env

# Configure Prefect to use dedicated server
prefect config set PREFECT_API_URL="http://${NODE_IP}:30420/api"

# Verify configuration
prefect config view
echo "RAY_ADDRESS=$RAY_ADDRESS"
```

### 4. Verify Connection

```bash
# Test Ray connection
python3 -c "import ray; ray.init(address='$RAY_ADDRESS'); print('✅ Ray connected:', ray.cluster_resources())"

# Test Prefect connection (should NOT show "temporary server")
python pipelines/demo_pipeline.py
```

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
source ~/zdb_deployment/.env

# Run a specific pipeline
python pipelines/demo_pipeline.py
python pipelines/api_to_delta_pipeline.py
python pipelines/kafka_to_delta_pipeline.py
```

---

## Troubleshooting

### "Starting temporary server" message
Prefect isn't using your dedicated server. Fix:
```bash
prefect config set PREFECT_API_URL="http://172.28.176.60:30420/api"
```

### "Creating a local Ray instance" message
Ray isn't connecting to the cluster. Fix:
```bash
# Make sure RAY_ADDRESS is set
source ~/zdb_deployment/.env
echo $RAY_ADDRESS  # Should show ray://IP:PORT

# Test connection
python3 -c "import ray; ray.init(address='$RAY_ADDRESS'); print(ray.cluster_resources())"
```

### Version mismatch error
Ray client version must match cluster. Check cluster version:
```bash
kubectl exec -n etl-compute $(kubectl get pods -n etl-compute -l app=ray-head -o name | head -1) -- pip show ray | grep Version
```
Then install matching version locally:
```bash
pip install ray==<version>
```

### ModuleNotFoundError: 'etl'
Install the package:
```bash
cd ~/zdb_deployment/app-code
pip install -e .
```
