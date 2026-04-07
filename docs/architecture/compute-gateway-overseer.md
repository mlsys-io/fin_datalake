# Compute, Gateway, And Overseer Interaction

This document describes the current interaction model between the compute plane, the gateway, and the overseer. It is an implementation-focused companion to the canonical 5-layer architecture in [../ARCHITECTURE.md](../ARCHITECTURE.md), not a competing architecture framing.

Within the 5-layer model:

- `compute` here primarily maps to the **Compute** and **Intelligence** layers
- `gateway` primarily maps to the **Interface** layer
- `overseer` primarily maps to the **Control** layer

## Layer Responsibilities

### Compute

The compute plane hosts:

- Ray tasks and actors
- Ray Serve applications for agents
- AgentHub for live discovery
- Prefect flows for orchestration

It is responsible for executing work and serving live agent endpoints.

### Gateway

The gateway is the user-facing API and dashboard backend. It:

- exposes platform operations to the frontend and API clients
- reads the durable catalog
- enriches catalog results with live runtime information from AgentHub when available
- provides a stable API even when live runtime introspection is temporarily degraded

### Overseer

The overseer is the control plane. It:

- collects runtime health and service metrics
- reads the durable catalog
- compares desired deployment state against live Serve state
- updates observed status back into the catalog
- plans and executes recovery or alert actions

## Shared State Model

The platform uses two complementary registries:

### AgentHub

AgentHub is the live runtime registry. It stores:

- registered deployment names
- capability metadata
- live handle-based liveness signals

It is optimized for current runtime discovery, not durability.

### Durable Catalog

The durable catalog stores:

- deployment definitions
- recovery metadata
- desired state
- observed state
- reconciliation notes

It is the system memory that survives cluster restarts and gives Overseer something durable to reconcile against.

## Deployment Workflow

1. An operator or script deploys an agent through `BaseAgent.deploy()` or the shared manager/CLI.
2. Ray Serve creates the application under a stable app name.
3. The agent registers itself with AgentHub.
4. The agent upserts its catalog row with deployment metadata and initial healthy state.
5. Gateway can list the deployment through `catalog+runtime`.
6. Overseer later uses the same catalog row as the durable definition for reconciliation.

## Gateway Listing Workflow

When the gateway serves `/api/v1/agents`, it:

1. attempts to read live runtime data from AgentHub
2. reads the durable catalog
3. merges both views into a stable API response

When runtime is healthy, agents appear as `catalog+runtime`.

When runtime listing is unavailable, the gateway can still return catalog-backed state instead of failing completely.

## Overseer Reconciliation Workflow

Each Overseer cycle does the following:

1. collectors gather service metrics
2. the Ray collector normalizes Serve applications into control-friendly summaries
3. Overseer loads the durable agent catalog
4. each catalog deployment is matched by `metadata.app_name` or `name`
5. Overseer merges desired catalog state with normalized live Serve state
6. the merged state is written back to the catalog
7. policies inspect the resulting `agent_control` view and plan actions
8. actuators execute respawn, alert, or reporting behavior

## Why Serve Normalization Lives In The Collector

Serve-specific parsing now lives in the Ray collector because:

- Serve dashboard payloads are runtime-specific
- the collector is the right place to interpret runtime-specific structures
- the Overseer loop should primarily reconcile catalog state against normalized live state

This keeps the loop smaller and makes the collector the source of truth for live Serve status interpretation.

## Healing Workflow

The healing workflow depends on the durable catalog:

1. a Serve app disappears
2. the catalog still says the deployment should be running
3. Overseer sees the deployment as missing or recovering
4. policy emits a `RESPAWN`
5. the actuator redeploys the same Serve app name
6. the Ray collector sees the app return
7. Overseer updates the catalog back to `ready` and clears stale failure text

## Intentional Deletion Workflow

Intentional deletion is different from failure recovery:

1. an operator deletes the Serve app intentionally
2. the operator also cleans the catalog entry or disables desired running state
3. because the durable desired-state record is gone, Overseer does not resurrect it

This is why deployment cleanup utilities remove both the Serve app and the durable catalog row when requested.

## Current Result

The current platform behavior is:

- Compute executes and hosts live agents
- Gateway presents a merged durable-plus-live view
- Overseer reconciles durable desired state against normalized live Serve state
- AgentHub remains the live registry, while the catalog remains the durable registry
