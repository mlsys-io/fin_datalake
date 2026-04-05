# ETL And Agent Framework Guide

This guide explains the framework layer used by the compute, gateway, and overseer services. It covers the ETL runtime, Serve-backed agents, the durable catalog, and the standard deployment management helpers.

## Core Building Blocks

### BaseTask

`BaseTask` is the unit-of-work abstraction for distributed ETL tasks. It is appropriate for finite compute steps that can be submitted and retried through Ray and Prefect.

### ServiceTask

`ServiceTask` is the base for long-running Ray actors and service-style workloads. It provides the deploy/connect pattern used by persistent components.

### BaseAgent

`BaseAgent` is the request-driven agent container for Ray Serve. Its main responsibilities are:

- deploy a Serve application with a stable app name
- register typed capabilities in AgentHub
- upsert a durable catalog entry
- expose `invoke`, `chat`, delegation, and event-handling paths

## Coordination Components

### AgentHub

AgentHub is the live runtime registry inside Ray. It provides:

- capability-based discovery
- live agent listing
- request routing by capability or explicit name
- lightweight liveness checks for Serve apps

AgentHub is intentionally live-only. It does not replace the durable catalog.

### Durable Agent Catalog

The catalog in `etl.agents.catalog` stores durable deployment records used by:

- Gateway, for catalog-backed listing and fallback behavior
- Overseer, for reconciliation and recovery
- demos and scripts, for checking control-plane state

Each entry stores both desired state and observed runtime state, including:

- `desired_status`
- `observed_status`
- `health_status`
- `recovery_state`
- `last_failure_reason`
- deployment metadata needed for respawn

## Deployment Management Helpers

The standard deployment lifecycle utilities now live in `etl.agents.manager`.

### Available Helpers

- `deploy_agent(...)`
- `deploy_fleet(...)`
- `deploy_baseline_fleet()`
- `delete_agent(...)`
- `delete_fleet(...)`
- `delete_baseline_fleet()`
- `list_fleet_state()`
- `baseline_fleet_specs()`

These helpers are deployment-aware rather than demo-specific. They use:

- `BaseAgent.deploy()` for creation
- `serve.delete(...)` for intentional removal
- catalog cleanup helpers when a deployment should not be restored by Overseer

### Baseline Fleet

The built-in baseline fleet currently consists of:

- `SupportAgent`
- `SentimentModel-1`
- `ForecastModel-1`
- `RouterAgent`

That same fleet definition is used by:

- the CLI
- `scripts/deploy_test_agents.py`
- operational workflows that need a stable baseline profile

## CLI Surface

The project exposes the lifecycle helper through the `etl-agents` command.

```bash
cd ~/zdb_deployment/app-code
uv run etl-agents deploy-baseline
uv run etl-agents list
uv run etl-agents delete SupportAgent --clean-catalog
```

This CLI is the recommended operator entrypoint for intentional agent lifecycle actions.

## Typical Interaction Flow

### Deploy

1. A deployment helper resolves the agent class.
2. `BaseAgent.deploy()` creates the Ray Serve app.
3. The agent registers itself with AgentHub.
4. The agent upserts its durable catalog entry.
5. Gateway and Overseer can now observe the deployment through both live and durable paths.

### Recover

1. The Serve app disappears or becomes unhealthy.
2. Overseer compares the catalog against normalized Serve state from the Ray collector.
3. Overseer plans a `RESPAWN` for managed desired deployments that are missing.
4. The actuator redeploys the same Serve app name using catalog metadata.

### Intentionally Delete

1. An operator deletes the Serve app through the manager or CLI.
2. The operator also cleans the catalog entry when the deployment should stay gone.
3. Because the desired-state record is removed, Overseer does not respawn it.

## Example

```python
from etl.agents.manager import deploy_agent, delete_agent, list_fleet_state

deploy_agent("SupportAgent", name="SupportAgent")
state = list_fleet_state()
delete_agent("SupportAgent", clean_catalog=True)
```

## Related Documents

- Workspace and operations guide: [../README.md](../README.md)
- Platform interaction model: [../../docs/architecture/compute-gateway-overseer.md](../../docs/architecture/compute-gateway-overseer.md)
- Final demo plan reference: [../../docs/FINAL_DEMO_PLAN.md](../../docs/FINAL_DEMO_PLAN.md)
