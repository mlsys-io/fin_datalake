# AI-Native Lakehouse ETL Framework

This framework provides a modular, platform-agnostic way to build distributed data pipelines and AI Agents. It leverages **Prefect** for orchestration and **Ray** for scalable computation.

## 🏗 Architecture Layers

1.  **Ingestion Layer (`etl.io.sources`)**: Fetches data from APIs, Streams, or Files.
2.  **Storage Layer (`etl.io.sinks`)**: Writers for Delta Lake (History), TimescaleDB (Metrics), and Milvus (Vectors).
3.  **Processing Layer (`etl.core`)**: Distributed Tasks (`@task`) and Stateful Services (`ServiceTask`) running on Ray.
4.  **Intelligence Layer (`etl.agents`)**: AI Agents hosted as persistent Ray Actors that can reason over data.

---

## 🧩 Component Reference

### 1. Data Sources (`etl.io.sources`)
Configuration objects that define *where* data comes from.
*   **`RestApiSource`**: Polls JSON APIs (News, Prices) with pagination and auth.
*   **`WebSocketSource`**: Connects to live streams and yields micro-batches.
*   **`FileSource`**: Reads large static files (CSV, Parquet) using Ray Data.
*   **`KafkaSource`**: Consumes events from Kafka topics with micro-batching.
*   **`RisingWaveSource`**: Reads from the Streaming Database.

### 2. Data Sinks (`etl.io.sinks`)
Configuration objects that define *where* data goes.
*   **`DeltaLakeSink`**: The "Source of Truth". Writes Parquet to S3/MinIO and **auto-registers to Hive**.
*   **`TimescaleDBSink`**: Optimized for high-frequency time-series metrics.
*   **`MilvusSink`**: Stores vector embeddings for RAG.
*   **`HttpSink`**: Sends data to webhooks/APIs with retry and auth support.

### 3. AI Agents (`etl.agents`)
*   **`BaseAgent`**: The "Container". Wrapped as a Ray Actor. Handles lifecycle and messaging.
*   **`LangChainAgent`**: A helper for Agents built with LangChain/LangGraph.
*   **`Tools`**: Pre-built wrappers for Sinks (e.g., `TimescaleTool`, `MilvusTool`).

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
from etl.sample_agents.market_analyst import MarketAnalystAgent

# Deploy on the Cluster
actor = MarketAnalystAgent.as_ray_actor(num_cpus=1).remote()

# Interact
response = ray.get(actor.ask.remote("What is the trend for AAPL?"))
print(response)
```

### C. Accessing Services in Pipelines
You can connect to running services (like Agents or the Hive Metastore) from within your Prefect Tasks using Ray's actor namespace.

```python
import ray
from etl.core.base_service import ServiceTask

@task
def ask_analyst_agent(question: str):
    # 1. Get the handle to the running 'MarketAnalystAgent' service
    # Note: Ensure the service was started previously
    try:
        analyst = ray.get_actor("MarketAnalystAgent")
    except ValueError:
        raise RuntimeError("MarketAnalystAgent is not running!")

    # 2. Interact with it
    answer = ray.get(analyst.ask.remote(question))
    return answer
```

### D. Event-Driven Pipeline with Kafka
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

### E. Sending Notifications via HTTP
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
    def build_executor(self):
        # Return any callable. Could be a pure function or a complex chain.
        def _analyze(text):
            return "Positive" if "good" in text else "Negative"
        return _analyze

# Deploy
actor = SentimentAgent.as_ray_actor().remote()
print(ray.get(actor.ask.remote("This is good code"))) # -> "Positive"
```
