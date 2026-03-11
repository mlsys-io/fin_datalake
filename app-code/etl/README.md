# AI-Native Lakehouse ETL Framework

This framework provides a modular, platform-agnostic way to build distributed data pipelines and AI Agents. It leverages **Prefect** for orchestration and **Ray** for scalable computation.

## 🏗 Architecture Layers

1.  **Ingestion Layer (`etl.io.sources`)**: Fetches data from APIs, Streams, Files, or Databases.
2.  **Storage Layer (`etl.io.sinks`)**: Writers for Delta Lake (History), TimescaleDB (Metrics), Milvus (Vectors), and HTTP endpoints.
3.  **Processing Layer (`etl.core`)**: Distributed Tasks (`BaseTask`) and Stateful Services (`ServiceTask`) running on Ray.
4.  **Services Layer (`etl.services`)**: Long-running services for streaming, Hive registration, MinIO management, etc.
5.  **Intelligence Layer (`etl.agents`)**: AI Agents hosted as persistent Ray Actors with coordination via AgentHub and ContextStore.

---

## 🧩 Component Reference

### 1. Data Sources (`etl.io.sources`)
Configuration objects that define *where* data comes from.
*   **`RestApiSource`**: Polls JSON APIs (News, Prices) with pagination and auth.
*   **`WebSocketSource`**: Connects to live streams and yields micro-batches.
*   **`FileSource`**: Reads large static files (CSV, Parquet) using Ray Data.
*   **`KafkaSource`**: Consumes events from Kafka topics with micro-batching.
*   **`DeltaLakeSource`** *(direct import)*: Reads from Delta Lake tables.
*   **`RisingWaveSource`** *(direct import)*: Reads from the Streaming Database.

### 2. Data Sinks (`etl.io.sinks`)
Configuration objects that define *where* data goes.
*   **`DeltaLakeSink`**: The "Source of Truth". Writes Parquet to S3/MinIO and **auto-registers to Hive**.
*   **`TimescaleDBSink`**: Optimized for high-frequency time-series metrics.
*   **`MilvusSink`**: Stores vector embeddings for RAG.
*   **`HttpSink`**: Sends data to webhooks/APIs with retry and auth support.

### 3. Processing & Services (`etl.core`, `etl.services`)
*   **`BaseTask`**: Unit of distributed work. Runs on Ray workers via `@ray.remote`.
*   **`ServiceTask`**: Long-running actor base class. Provides `deploy()`, `connect()`, and `SyncHandle`.
*   **`StatefulProcessorService`**: Streaming service with windowed aggregations (WebSocket → TimescaleDB).

### 4. AI Agents (`etl.agents`)
*   **`BaseAgent`**: The "Container". Deploy as a Ray Actor via `deploy()`. Handles lifecycle, messaging, and coordination.
*   **`LangChainAgent`**: A helper for Agents built with LangChain/LangGraph.
*   **`Tools`**: Pre-built wrappers for Sinks (e.g., `TimescaleTool`, `MilvusTool`).

### 5. Agent Coordination (`etl.agents`)
*   **`AgentHub`**: Central coordination — service discovery, synchronous routing, notifications, health monitoring.
*   **`ContextStore`**: Shared key-value state with TTL for cross-agent collaboration.
*   **`SyncHandle`**: Proxy that removes `ray.get`/`.remote()` boilerplate. Supports `async_` prefix for fire-and-forget.

---

## 🚀 Usage Guide

### A. Building a Data Pipeline
Combine a Source and a Sink in a Prefect Flow.

```python
from prefect import flow
from prefect_ray.task_runners import RayTaskRunner
from etl.io.sources.rest_api import RestApiSource
from etl.io.sinks.delta_lake import DeltaLakeSink

@flow(task_runner=RayTaskRunner)
def ingest_news():
    # 1. Define Config
    source = RestApiSource(url="https://api.market.com/news", auth=...)
    sink = DeltaLakeSink(uri="s3://lake/news", hive_table_name="default.news")

    # 2. Run Task (Generic Ingestion Logic)
    ingest_task.submit(source, sink)
```

### B. Deploying an AI Agent
Define the Agent logic and deploy it as a persistent service.

```python
from sample_agents.market_analyst import MarketAnalystAgent

# Deploy on the Cluster — returns SyncHandle
agent = MarketAnalystAgent.deploy(name="MarketAnalyst", num_cpus=1)

# Interact — no ray.get/.remote() needed
response = agent.ask("What is the trend for AAPL?")
print(response)

# Fire-and-forget (async_ prefix)
agent.async_notify({"topic": "alert", "payload": {"msg": "New earnings data"}})
```

### C. Accessing Services in Pipelines
Connect to running agents from within your Prefect Tasks.

```python
from etl.agents import BaseAgent

@task
def ask_analyst_agent(question: str):
    # connect() retrieves an existing actor by name — returns SyncHandle
    analyst = BaseAgent.connect("MarketAnalyst")
    return analyst.ask(question)
```

### D. Agent Coordination
Agents coordinate via AgentHub and ContextStore.

```python
from etl.agents import get_hub, get_context
from etl.core.base_service import SyncHandle

hub = SyncHandle(get_hub())
ctx = SyncHandle(get_context())

# Discovery + routing
analysts = hub.find_by_capability("analysis")
result = hub.call_by_capability("analysis", payload)

# Notifications
hub.notify_capability("analysis", event)

# Shared state with TTL
ctx.set("signal:aapl", "BUY", owner="AnalystAgent", ttl=300)
signal = ctx.get("signal:aapl")
```

### E. Event-Driven Pipeline with Kafka
Consume from Kafka and write to Delta Lake.

```python
from etl.io.sources.kafka import KafkaSource
from etl.io.sinks.delta_lake import DeltaLakeSink

@flow
def kafka_to_delta():
    source = KafkaSource(
        bootstrap_servers="localhost:9092",
        topics=["events"],
        batch_size=100
    )
    sink = DeltaLakeSink(uri="s3://lake/events")
    
    with source.open() as reader:
        with sink.open() as writer:
            for batch in reader.read_batch():
                writer.write_batch(batch)
```

### F. Deploying a Streaming Service
Deploy a stateful processor with monitoring.

```python
from etl.services.processing.stateful_processor import StatefulProcessorService

svc = StatefulProcessorService.deploy(
    name="CryptoProcessor",
    config={
        "source_config": {"url": "wss://ws.bitstamp.net"},
        "sink_config": {"host": "localhost", "database": "mydb", "table_name": "crypto"},
        "window_seconds": 5,
    },
)

svc.async_run()              # Start streaming loop (fire-and-forget)
status = svc.get_status()    # Poll metrics
svc.stop()                   # Graceful shutdown
```

### G. Sending Notifications via HTTP
Send processed data to a webhook endpoint.

```python
from etl.io.sinks.http import HttpSink

sink = HttpSink(
    url="https://api.slack.com/webhook",
    auth_token="xoxb-token",
    batch_key="events"
)

with sink.open() as writer:
    writer.write_batch([{"event": "pipeline_complete", "count": 100}])
```

---

## 🛠 Developer Guide: Extending the Framework

You can easily create your own components by inheriting from the base classes.

### 1. Creating a Custom Source
Use `DataSource` for config and `DataReader` for runtime logic.

```python
from etl.io.base import DataSource, DataReader

# 1. The Config (Serializable)
@dataclass
class TwitterSource(DataSource):
    api_key: str
    query: str
    
    def open(self):
        return TwitterReader(self)

# 2. The Runtime (Worker)
class TwitterReader(DataReader):
    def read_batch(self):
        # Connect and yield lists of dicts
        client = TwitterClient(self.source.api_key)
        yield client.search(self.source.query)
```

### 2. Creating a Custom Sink
Use `DataSink` and `DataWriter`.

```python
from etl.io.base import DataSink, DataWriter

# 1. The Config
@dataclass
class SlackSink(DataSink):
    webhook_url: str
    
    def open(self):
        return SlackWriter(self)

# 2. The Runtime
class SlackWriter(DataWriter):
    def write_batch(self, data):
        # Post data to Slack
        requests.post(self.sink.webhook_url, json={"text": str(data)})
```

### 3. Creating a Custom Agent
Inherit from `BaseAgent` (or `LangChainAgent`) and implement `build_executor`.

```python
from etl.agents import BaseAgent

class SentimentAgent(BaseAgent):
    CAPABILITIES = ["sentiment"]

    def build_executor(self):
        # Return any callable. Could be a pure function or a complex chain.
        def _analyze(text):
            return "Positive" if "good" in text else "Negative"
        return _analyze

# Deploy — one-liner, returns SyncHandle
agent = SentimentAgent.deploy(name="SentimentAgent")
print(agent.ask("This is good code"))  # -> "Positive"

# From another process:
agent = SentimentAgent.connect("SentimentAgent")
agent.ask("Market looks bad")  # -> "Negative"

# Graceful shutdown
agent.shutdown()
```
