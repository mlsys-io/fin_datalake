# AI-Native Lakehouse Architecture

This document is the canonical architecture reference for the current repository state. It treats the codebase as the source of truth and uses the **five-layer model** as the primary framing:

- Data
- Compute
- Intelligence
- Control
- Interface

It is intentionally different from older repo notes and from `final_report/main.tex` where the draft report still reflects earlier design assumptions in several places.

## Architecture Summary

The platform co-locates ETL workloads and Serve-backed agents on shared Ray infrastructure, then wraps that runtime with a durable control and access layer:

- The **Data layer** standardizes ingestion and storage contracts around `DataSource` and `DataSink`.
- The **Compute layer** runs Prefect flows, Ray tasks, and distributed or local execution paths depending on workload needs.
- The **Intelligence layer** hosts long-lived Ray Serve agents, with AgentHub providing live discovery and the durable catalog preserving deployment memory.
- The **Control layer** runs Overseer as an out-of-band reconciliation loop that compares desired catalog state with normalized live Serve state.
- The **Interface layer** exposes the system through REST and MCP, with shared dispatch, RBAC, audit logging, and dashboard-oriented APIs.

## Layer Model

### Data Layer

The data layer is the ingestion and storage contract surface. Its main responsibilities are:

- source abstraction through `DataSource`
- sink abstraction through `DataSink`
- Delta Lake writes
- optional downstream sinks such as TimescaleDB and Milvus
- Hive metastore registration where configured

Current implementation notes:

- The repo contains concrete sources for REST, WebSocket, Kafka, files, Delta Lake, and RisingWave.
- The repo contains sinks for Delta Lake, TimescaleDB, HTTP, DLQ, and Milvus.
- Some older report language around adaptive schema evolution is directionally consistent, but the exact behavior should be described from the sink implementations that exist now.

### Compute Layer

The compute layer is the execution substrate for ETL and orchestration. It includes:

- Ray runtime and Ray client connectivity
- Ray Serve deployment hosting
- Prefect flows and task runners
- BaseTask helpers for distributed and local execution

Current implementation notes:

- Prefect is present, but the repo does **not** use Ray in one universal way. Some flows use `RayTaskRunner`, while others use Prefect-only concurrency such as `ConcurrentTaskRunner`.
- The hybrid execution story should be described carefully. The repo still supports explicit local execution via `BaseTask.local()` and `DeltaLakeWriteTask.local()`, but it also supports distributed Delta writes through Ray Data.
- It is therefore more accurate to describe the compute layer as supporting **multiple execution patterns on the same platform**, not as a single universal Prefect-plus-Ray path.

### Intelligence Layer

The intelligence layer is the long-lived agent runtime. It includes:

- `BaseAgent` as the Serve-backed agent container
- Ray Serve application deployment with stable app names
- typed capability registration
- direct invoke, chat, event, and delegation paths
- ContextStore for shared short-lived state
- AgentHub for live discovery and liveness-oriented metadata
- the durable agent catalog for deployment memory across failures

Current implementation notes:

- The repo does **not** match the stronger claim that AgentHub has been fully displaced by a decentralized service mesh.
- The current model is better described as **centralized live discovery plus direct Serve invocation**:
  - AgentHub remains the live capability registry and liveness-oriented discovery surface.
  - Delegation resolves candidates through AgentHub.
  - Invocation then happens directly against Ray Serve handles.
- ContextStore remains relevant for shared ephemeral state such as published signals.

### Control Layer

The control layer is the out-of-band autonomic management plane. It includes:

- Overseer collectors
- policy evaluation
- cooldown handling
- action execution through actuators
- snapshot and alert persistence
- catalog reconciliation

Current implementation notes:

- Overseer is out-of-band from the managed workloads, but it does **not** communicate only through the Ray HTTP dashboard API. It also uses Ray runtime/client helpers and writes back to the durable catalog.
- The live configured polling interval is currently **5 seconds** in `app-code/overseer/config.yaml`, not 15 seconds.
- The code clearly implements:
  - deployment state normalization from Ray collector output
  - agent deployment reconciliation
  - same-name respawn for missing managed Serve apps
  - alert logging and status reporting
  - gateway circuit-breaker actions
- The docs should not overstate this as full cluster-level recovery orchestration unless such behavior is actually implemented and verified in code.

### Interface Layer

The interface layer is the governed access surface for users, dashboards, and AI clients. It includes:

- FastAPI REST API
- MCP server and tool exposure
- shared dispatch pipeline
- adapter registry
- auth and RBAC
- audit logging
- readiness and system endpoints
- frontend-facing routes and dashboard support

Current implementation notes:

- The repo clearly implements **REST and MCP** as gateway interfaces.
- The repo also contains operator CLIs such as `etl-agents`, but that is not the same as a generic gateway CLI translator peer to REST and MCP.
- The gateway registry currently exposes these domains:
  - `data`
  - `compute`
  - `agent`
  - `broker`
  - `system`
- `BrokerAdapter` currently vends environment-backed credentials and connection strings. It should not be described as already issuing short-lived scoped credentials because the code still marks that as future work.

## Cross-Layer State Model

Two registries are central to the current design:

- **AgentHub** is the live runtime registry inside Ray.
- The **durable agent catalog** is the deployment memory used by gateway and Overseer.

They serve different purposes:

- AgentHub answers live discovery and liveness questions.
- The durable catalog preserves desired state, observed state, reconciliation notes, and respawn metadata across failures and restarts.

This distinction is one of the most important differences between the current implementation and older documentation that treated runtime coordination as the only source of truth.

## Operator View vs Canonical View

Some existing docs and scripts use a narrower operational framing of:

- compute
- gateway
- overseer

That view is still useful for day-to-day operations, but it is a **projection** of the canonical five-layer model rather than a competing architecture:

- `gateway` primarily implements the **Interface layer**
- `overseer` primarily implements the **Control layer**
- `app-code/etl`, `pipelines`, and Ray infrastructure collectively span the **Data**, **Compute**, and **Intelligence** layers

## Current Boundaries And Non-Claims

The current repo documentation should explicitly avoid these overstatements:

- claiming a fully implemented generic gateway CLI peer interface
- claiming short-lived scoped broker credentials when the implementation still uses static/env-backed values
- claiming AgentHub is no longer central to live discovery
- claiming Overseer only talks to the Ray dashboard API
- claiming verified cluster-wide recovery orchestration when the implemented and demonstrated recovery path is deployment-level reconciliation and respawn

## Related Documents

- [architecture/compute-gateway-overseer.md](architecture/compute-gateway-overseer.md): implementation-focused interaction model for compute, interface, and control components
- [FINAL_DEMO_PLAN.md](FINAL_DEMO_PLAN.md): final demo narrative and operator sequence
- [REPORT_ARCHITECTURE_DELTA.md](REPORT_ARCHITECTURE_DELTA.md): report-draft discrepancy note for external editing support
