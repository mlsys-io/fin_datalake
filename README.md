# AI-Native Lakehouse

An AI-native data platform that combines distributed ETL, Ray Serve agents, a unified gateway, and an autonomic control plane on Kubernetes.

## What This Repository Contains

This repo is organized around three platform layers that now work together as one deployment lifecycle:

- Compute: Ray, Ray Serve, Prefect, ETL tasks, and AI agents.
- Gateway: the unified API and dashboard backend that exposes data, compute, and agent operations.
- Overseer: the control plane that reconciles durable desired state with live runtime state and performs recovery.

The durable agent catalog lives alongside these layers and gives the platform a shared deployment memory across restarts and failures.

## Architecture At A Glance

- Compute executes work and hosts Serve-backed agents.
- Gateway exposes the platform to operators and UI clients.
- Overseer monitors services, reconciles agent state, and triggers healing actions.
- The catalog stores durable agent definitions and runtime reconciliation status.
- AgentHub provides live capability discovery inside the Ray runtime.

For the detailed interaction model, read [compute-gateway-overseer.md](docs/architecture/compute-gateway-overseer.md).

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
- Architecture overview and subsystem narratives: [docs/README.md](docs/README.md)
- Control-plane interaction model: [docs/architecture/compute-gateway-overseer.md](docs/architecture/compute-gateway-overseer.md)
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
- Gateway reads from the durable catalog and enriches from live runtime state when available.
- Overseer reconciles catalog desired state against normalized Serve application state from the Ray collector.
- The self-healing demo uses the same deployment lifecycle utilities as the rest of the platform.
- The preferred correctness-check entrypoint now lives under `scripts/verify/`.
