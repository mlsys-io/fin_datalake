# 🏗 The AI Lakehouse — An AI-Native Data Platform

A cloud-native platform that unifies **distributed ETL**, **AI Agents**, and **real-time streaming** on Kubernetes. Built for a Final Year Project demonstrating how modern data infrastructure can be designed for usability, maintainability, and intelligent automation.

**Author**: Lian Zhi Xuan (Garret)

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Deployment Guide](#deployment-guide)
- [Usage Guide](#usage-guide)
- [Developer Guide](#developer-guide)
- [Environment Configuration](#environment-configuration)

---

## Architecture Overview

The system follows a layered **Cloud-Native Architecture** running on bare-metal Kubernetes (k0s).

```
┌──────────────────────────────────────────────────────────────────┐
│                    🌐 Interface Layer                            │
│         Gateway (REST API / MCP / WebSocket)                     │
│         Frontend Dashboard (React + Vite)                        │
├──────────────────────────────────────────────────────────────────┤
│                    🧠 Intelligence Layer                         │
│  BaseAgent → deploy() / connect() → SyncHandle                  │
│  AgentHub │ ContextStore │ SyncHandle                           │
├──────────────────────────────────────────────────────────────────┤
│                    🔄 Compute & Orchestration Layer               │
│       Ray Cluster (Actors + Tasks)  │  Prefect (Workflows)      │
│       ServiceTask → deploy() / connect()                        │
├──────────────────────────────────────────────────────────────────┤
│                    🛡 Autonomic Layer                             │
│         System Overseer (MAPE-K Control Loop)                    │
│         Collectors │ Policies │ Actuators                        │
├──────────────────────────────────────────────────────────────────┤
│                    💾 Storage Layer                               │
│  Delta Lake (MinIO S3) │ Hive Metastore │ TimescaleDB │ Milvus  │
├──────────────────────────────────────────────────────────────────┤
│                    ⚙️ Infrastructure Layer                        │
│              k0s Kubernetes │ Helm │ MinIO (TLS/SSL)             │
└──────────────────────────────────────────────────────────────────┘
```

### Layer Descriptions

| Layer | Components | Role |
|---|---|---|
| **Interface** | Gateway, Frontend | User-facing API and dashboard. Routes requests via InterfaceRegistry to domain adapters (Data, Compute, Agent, Broker). |
| **Intelligence** | BaseAgent, AgentHub, ContextStore | AI Agents as persistent Ray Actors with service discovery, synchronous routing, notifications, and shared state. |
| **Compute** | Ray, Prefect, ServiceTask, BaseTask | Distributed execution engine. Ray runs tasks and actors; Prefect manages workflows, retries, and scheduling. |
| **Autonomic** | System Overseer | MAPE-K control loop that monitors services, detects failures, and auto-scales agents. |
| **Storage** | Delta Lake, MinIO, Hive, TimescaleDB, Milvus | ACID data lake (Delta), time-series metrics (TimescaleDB), vector embeddings (Milvus), schema catalog (Hive). |
| **Infrastructure** | k0s, Helm, MinIO | Bare-metal Kubernetes with TLS-secured object storage. |

---

## Project Structure

```
zdb_deployment/
├── app-code/                      # All application code
│   ├── etl/                       # Core ETL framework
│   │   ├── core/                  #   BaseTask, ServiceTask, SyncHandle
│   │   ├── io/                    #   sources/ (REST, WebSocket, Kafka, File, Delta, RisingWave)
│   │   │                          #   sinks/   (Delta Lake, TimescaleDB, Milvus, HTTP)
│   │   ├── agents/                #   BaseAgent, AgentHub, ContextStore
│   │   ├── services/              #   StatefulProcessorService, Hive, MinIO services
│   │   └── docs/                  #   Design documents
│   ├── gateway/                   # Unified API Gateway
│   │   ├── api/                   #   FastAPI routes + auth
│   │   ├── adapters/              #   Domain adapters (agent, data, compute, broker)
│   │   ├── mcp/                   #   Model Context Protocol server
│   │   └── core/                  #   InterfaceRegistry, BaseAdapter
│   ├── overseer/                  # System Overseer (Autonomic Manager)
│   │   ├── collectors/            #   Ray, Kafka, Prefect, Health probes
│   │   ├── policies/              #   Auto-scale, self-heal rules
│   │   └── actuators/             #   Provisioning actions
│   ├── frontend/                  # Admin Dashboard (React + Vite)
│   ├── pipelines/                 # Prefect pipeline definitions
│   ├── sample-agents/             # Example AI agents (MarketAnalyst)
│   └── transformations/           # Reusable data transformations
├── deps/                          # Kubernetes dependency configs
│   ├── hive/                      #   Hive Metastore + PostgreSQL Helm charts
│   ├── kuberay/                   #   KubeRay operator + RayCluster manifests
│   ├── minio/                     #   MinIO configuration
│   ├── prefect/                   #   Prefect server Helm values
│   ├── timescaledb/               #   TimescaleDB (Crunchy PGO)
│   ├── risingwave/                #   RisingWave streaming DB
│   └── milvus/                    #   Milvus vector DB
├── demo-sources/                  # Demo data sources (Kafka, API, WebSocket, Static)
├── app/                           # RayJob YAML and run script
├── deploy-all.sh                  # Master deployment script
├── setup-config.sh                # Initial cluster configuration
├── cleanup.sh                     # Full cluster teardown
└── .env.example                   # Environment variable template
```

---

## Prerequisites

- **Two Servers** (or one with sufficient resources):
  - Compute node: runs k0s Kubernetes cluster
  - Storage node: runs MinIO (S3-compatible storage with TLS)
- **Software**: `kubectl`, `helm`, `curl`, Python 3.11+, `uv` (package manager)
- **Optional**: Node.js 18+ (for frontend dashboard)

---

## Deployment Guide

### 1. Install Kubernetes (k0s)

```bash
# Install k0s
curl --proto '=https' --tlsv1.2 -sSf https://get.k0s.sh | sudo sh
sudo k0s install controller --single
sudo k0s start

# Install kubectl
curl -LO https://dl.k8s.io/$(k0s version | sed -r "s/\+k0s\.[[:digit:]]+$//")/kubernetes-client-linux-amd64.tar.gz
tar -zxvf kubernetes-client-linux-amd64.tar.gz kubernetes/client/bin/kubectl
sudo mv kubernetes/client/bin/kubectl /usr/local/bin/kubectl

# Configure kubeconfig
mkdir -p $HOME/.kube
sudo cp /var/lib/k0s/pki/admin.conf $HOME/.kube/config
sudo chown $USER $HOME/.kube/config

# Install Helm
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your server IPs, MinIO credentials, etc.
source .env
```

### 3. Deploy All Services

```bash
# Deploy everything (namespaces, Ray, Prefect, TimescaleDB, RisingWave, Milvus, demo sources)
./deploy-all.sh all

# Or deploy individual components
./deploy-all.sh kuberay       # Ray cluster
./deploy-all.sh prefect       # Prefect orchestrator
./deploy-all.sh timescaledb   # TimescaleDB
./deploy-all.sh risingwave    # RisingWave streaming DB
./deploy-all.sh milvus        # Milvus vector DB
./deploy-all.sh sources       # Demo data sources

# Hive requires MinIO credentials
export MINIO_ENDPOINT=https://your-minio:4000
export MINIO_USER=admin
export MINIO_PASSWORD=secret
./deploy-all.sh hive

# Check status
./deploy-all.sh status
```

### 4. Set Up MinIO TLS (Storage Node)

Register the MinIO CA certificate with the K8s cluster:
```bash
kubectl delete configmap minio-ca 2>/dev/null
kubectl create configmap minio-ca --from-file=public.crt=$MINIO_CERT
kubectl delete secret minio-creds 2>/dev/null
kubectl create secret generic minio-creds \
  --from-literal=username=$MINIO_USERNAME --from-literal=password=$MINIO_PASSWORD
```

### Kubernetes Namespace Layout

| Namespace | Services |
|---|---|
| `etl-compute` | KubeRay operator, Ray cluster |
| `etl-orchestrate` | Prefect server, Prefect worker |
| `etl-storage` | Hive Metastore, PostgreSQL (HMS backend) |
| `etl-data` | TimescaleDB, RisingWave, Milvus |
| `demo-sources` | Kafka, WebSocket, REST API, Static file server |

---

## Usage Guide

### A. Running ETL Pipelines

Pipelines are defined as Prefect flows in `app-code/pipelines/`.

```bash
cd app-code

# Run the demo pipeline
uv run python -m pipelines.demo_pipeline

# Run specific pipelines
uv run python -m pipelines.api_to_delta_pipeline
uv run python -m pipelines.kafka_to_delta_pipeline
```

#### Building a Pipeline

```python
from prefect import flow
from etl.io.sources.rest_api import RestApiSource
from etl.io.sinks.delta_lake import DeltaLakeSink

@flow
def ingest_news():
    source = RestApiSource(url="https://api.example.com/news")
    sink = DeltaLakeSink(uri="s3://delta-lake/bronze/news")

    with source.open() as reader:
        with sink.open() as writer:
            for batch in reader.read_batch():
                writer.write_batch(batch)
```

### B. Deploying AI Agents

Agents are deployed as persistent Ray Actors with full remote method access.

```python
from etl.agents import BaseAgent

class AnalystAgent(BaseAgent):
    CAPABILITIES = ["analysis", "market_data"]

    def build_executor(self):
        def analyze(payload):
            return {"verdict": "BUY", "confidence": 0.85}
        return analyze

# Deploy — returns SyncHandle (no ray.get/.remote() boilerplate)
agent = AnalystAgent.deploy(name="AnalystAgent")

# Sync call — blocks until result
result = agent.ask("Should I buy AAPL?")

# Fire-and-forget — returns immediately
agent.async_notify({"topic": "market_update", "symbol": "AAPL"})

# Graceful shutdown — deregisters from AgentHub, releases resources
agent.shutdown()
```

#### Connecting from Another Process

```python
# From any script connected to the same Ray cluster
agent = AnalystAgent.connect("AnalystAgent")
result = agent.ask("Re-check AAPL")
```

### C. Agent Coordination

Agents coordinate through the **AgentHub** (service discovery + routing) and **ContextStore** (shared state):

```python
from etl.agents import get_hub, get_context
from etl.core.base_service import SyncHandle

hub = SyncHandle(get_hub())
ctx = SyncHandle(get_context())

# 1. Service Discovery — find agents by capability
analysts = hub.find_by_capability("analysis")    # → ["AnalystAgent"]

# 2. Synchronous Routing — call agents by name or capability
result = hub.call("AnalystAgent", payload)            # by name
result = hub.call_by_capability("analysis", payload)  # by capability

# 3. Notifications — fire-and-forget
hub.notify("AnalystAgent", event)                 # to one agent
hub.notify_capability("analysis", event)           # by capability
hub.notify_all(event)                              # broadcast all

# 4. Shared State — key-value store with TTL
ctx.set("signal:aapl", "BUY", owner="AnalystAgent", ttl=300)
signal = ctx.get("signal:aapl")

# 5. Delegation — route tasks by capability (from within an agent)
result = agent.delegate("analysis", "Analyze MSFT fundamentals")

# 6. Health Monitoring
hub.health_check()   # → {"AnalystAgent": True, "DataAgent": False}
```

#### Running the Agent Coordination Demo

```bash
cd app-code
uv run python -m pipelines.agent_coordination_demo
```

This verifies all coordination components: AgentHub, ContextStore, Delegation, Notifications, and Graceful Shutdown.

### D. Deploying Streaming Services

Deploy long-running streaming services with monitoring.

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

### E. Gateway API

<!-- 🚧 PLACEHOLDER: Gateway usage guide -->

The Gateway provides a unified API layer for interacting with the platform. It routes requests through the **InterfaceRegistry** to domain-specific adapters:

| Adapter | Domain | Actions |
|---|---|---|
| `AgentAdapter` | AI Agents | `chat`, `list`, `status` |
| `DataAdapter` | Data Lake | `query`, `list_tables`, `preview` |
| `ComputeAdapter` | Ray / Pipelines | `submit_job`, `status`, `list_pipelines` |
| `AgentAdapter` | AI Agents | `chat`, `notify`, `list` |

```bash
# Start the Gateway API server
cd app-code
uv run uvicorn gateway.api.main:app --host 0.0.0.0 --port 8000
```

> **Status**: Gateway adapters are implemented. REST API routes and MCP server are functional. Full API documentation will be added after end-to-end integration testing.

### F. System Overseer

<!-- 🚧 PLACEHOLDER: Overseer usage guide -->

The Overseer is an autonomic monitoring service that implements the **MAPE-K control loop** (Monitor → Analyze → Plan → Execute).

| Component | Role | Implementation |
|---|---|---|
| **Collectors** | Probe services for metrics | `RayCollector`, `KafkaCollector`, `PrefectCollector`, `HealthCollector` |
| **Policies** | Decision rules | Auto-scale agents on Kafka lag, self-heal dead actors |
| **Actuators** | Execute provisioning actions | Scale Ray actors, restart services |

```bash
# Start the Overseer
cd app-code
uv run python -m overseer.main
```

> **Status**: Collector framework and policy engine are implemented. Auto-scaling and self-healing policies need end-to-end validation on the cluster.

### G. Frontend Dashboard

<!-- 🚧 PLACEHOLDER: Frontend usage guide -->

A React + Vite admin dashboard for monitoring the platform.

```bash
cd app-code/frontend
npm install
npm run dev        # Development server at http://localhost:5173
npm run build      # Production build
```

> **Status**: Dashboard scaffold and initial pages are built. Needs integration with the Gateway API for live data.

---

## Developer Guide

### Extending the Framework

#### Creating a Custom Source

```python
from etl.io.base import DataSource, DataReader
from dataclasses import dataclass

@dataclass
class TwitterSource(DataSource):
    api_key: str
    query: str

    def open(self):
        return TwitterReader(self)

class TwitterReader(DataReader):
    def read_batch(self):
        client = TwitterClient(self.source.api_key)
        yield client.search(self.source.query)
```

#### Creating a Custom Agent

```python
from etl.agents import BaseAgent

class SentimentAgent(BaseAgent):
    CAPABILITIES = ["sentiment"]

    def build_executor(self):
        def _analyze(text):
            return "Positive" if "good" in text else "Negative"
        return _analyze

# Deploy, use, connect, shutdown
agent = SentimentAgent.deploy(name="SentimentAgent")
print(agent.ask("This is good code"))  # → "Positive"
agent.shutdown()
```

### Key Classes & APIs

| Class | Location | Purpose |
|---|---|---|
| `BaseTask` | `etl.core.base_task` | Unit of distributed work (finite) |
| `ServiceTask` | `etl.core.base_service` | Long-running actor base class |
| `SyncHandle` | `etl.core.base_service` | Sync proxy — removes `ray.get`/`.remote()` boilerplate |
| `BaseAgent` | `etl.agents.base` | AI Agent container with lifecycle hooks |
| `AgentHub` | `etl.agents.hub` | Central coordination: discovery, routing, notifications |
| `ContextStore` | `etl.agents.context` | Shared key-value state with TTL |
| `InterfaceRegistry` | `gateway.core.registry` | Routes API requests to domain adapters |

### SyncHandle API Reference

All `deploy()` and `connect()` calls return a `SyncHandle`:

```python
agent = MyAgent.deploy(name="MyAgent")

agent.ask("question")           # Sync  — blocks, returns result
agent.async_notify(event)       # Async — fire-and-forget, returns ObjectRef
agent.handle.notify.remote(ev)  # Raw   — full Ray ActorHandle control
```

---

## Environment Configuration

Copy `.env.example` to `.env` and configure:

| Variable | Description |
|---|---|
| `K8S_NODE_IP` | Kubernetes compute node IP |
| `STORAGE_HOST` | MinIO storage node hostname |
| `AWS_ACCESS_KEY_ID` | MinIO access key |
| `AWS_SECRET_ACCESS_KEY` | MinIO secret key |
| `AWS_ENDPOINT_URL` | MinIO HTTPS endpoint |
| `RAY_ADDRESS` | Ray cluster connection string |
| `PREFECT_API_URL` | Prefect server API URL |
| `HIVE_HOST` / `HIVE_PORT` | Hive Metastore connection |
| `DELTA_ROOT` | Default Delta Lake S3 path |

---

## License

This project is developed as part of a Final Year Project at the National University of Singapore.