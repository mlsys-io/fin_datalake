# AI-Native Lakehouse

An AI-native data platform that combines distributed ETL, Ray Serve agents, a governed access layer, and an autonomic control plane on Kubernetes.

## What This Repository Contains

This repo is organized around a **five-layer architecture**:

- **Data**: source and sink abstractions, Delta Lake, and storage-facing ETL contracts.
- **Compute**: Prefect flows, Ray execution, and Serve deployment hosting.
- **Intelligence**: long-lived agents, AgentHub live discovery, ContextStore, and the durable agent catalog.
- **Control**: Overseer collectors, policies, actuators, and reconciliation logic.
- **Interface**: REST, MCP, dispatch, auth, and dashboard-facing APIs.

The durable agent catalog and AgentHub work together across these layers:

- AgentHub is the live runtime registry inside Ray.
- The durable catalog preserves desired state and reconciliation history across failures and restarts.

## Architecture At A Glance

- The **Data layer** standardizes ingestion and write paths through `DataSource` and `DataSink`.
- The **Compute layer** runs ETL and orchestration workloads through Ray, Prefect, and mixed execution patterns.
- The **Intelligence layer** hosts Serve-backed agents and supports live capability discovery plus direct invocation.
- The **Control layer** reconciles durable desired state against normalized live runtime state and triggers recovery actions.
- The **Interface layer** exposes the platform through REST and MCP, with shared governance through dispatch, RBAC, and audit logging.

Read these docs in order:

- Canonical architecture reference: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- Current compute/interface/control interaction model: [docs/architecture/compute-gateway-overseer.md](docs/architecture/compute-gateway-overseer.md)
- Report-draft delta note: [docs/REPORT_ARCHITECTURE_DELTA.md](docs/REPORT_ARCHITECTURE_DELTA.md)

## Quick Start Paths

- Local application workspace: [app-code/README.md](app-code/README.md)
- ETL and agent framework guide: [app-code/etl/README.md](app-code/etl/README.md)
- Architecture and narrative docs: [docs/README.md](docs/README.md)

## Common Operator Flows

- Deploy the baseline fleet:

```bash
bash ./scripts/verify/01-deploy-baseline.sh
```

- Run the control-plane smoke check:

```bash
bash ./scripts/verify/02-control-plane-smoke.sh
```

- Run the full verification pass:

```bash
bash ./scripts/verify/run-all.sh
```

## Documentation Map

- Workspace setup and day-to-day commands: [app-code/README.md](app-code/README.md)
- ETL, BaseAgent, AgentHub, and deployment helpers: [app-code/etl/README.md](app-code/etl/README.md)
- Architecture overview and doc index: [docs/README.md](docs/README.md)
- Canonical architecture reference: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- Compute, interface, and control interaction model: [docs/architecture/compute-gateway-overseer.md](docs/architecture/compute-gateway-overseer.md)
- Report reconciliation note: [docs/REPORT_ARCHITECTURE_DELTA.md](docs/REPORT_ARCHITECTURE_DELTA.md)
- Final demo plan: [docs/FINAL_DEMO_PLAN.md](docs/FINAL_DEMO_PLAN.md)
- Verification scripts and run order: [scripts/verify/README.md](scripts/verify/README.md)

## Repository Layout

```text
zdb_deployment/
|- app-code/       Application code: ETL, agents, gateway, overseer, frontend
|- deps/           Kubernetes manifests, Dockerfiles, and build scripts
|- docs/           Architecture notes, design narratives, and demo guides
|- demo-sources/   Demo inputs and supporting assets
|- scripts/        Wrapper scripts for demos and operations
```

## Current Platform Status

- Shared agent deployment management utility and CLI are available through `etl.agents.manager` and `etl-agents`.
- Gateway currently exposes REST and MCP interfaces; operator CLIs remain separate tooling rather than a generic third gateway translator.
- Gateway reads from the durable catalog and enriches from live runtime state when available.
- Overseer reconciles catalog desired state against normalized Serve application state from the Ray collector.
- AgentHub remains the live discovery registry for capability lookup and liveness-oriented runtime state.
- The self-healing demo uses the same deployment lifecycle utilities as the rest of the platform.
- The preferred correctness-check entrypoint now lives under `scripts/verify/`.
