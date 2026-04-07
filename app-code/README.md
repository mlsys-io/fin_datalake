# Application Workspace Guide

This directory contains the runnable platform code: ETL framework modules, Ray Serve agents, the gateway, the overseer, pipelines, and the frontend.

For final-project packaging, the client-facing core API now also ships as the
`lakehouse_client` package with a matching `lakehouse-client` CLI entrypoint.
At the moment this package is consumed from the repository workspace via an
editable install rather than from a published package index.

## Environment Setup

Use Python 3.12 for the client environment if you connect to the Ray cluster through Ray Client.

This is the recommended path for users who want to author ETL jobs, invoke the
agent-management CLI, or integrate with the platform SDK while the project is
distributed as source.

```bash
cd ~/zdb_deployment/app-code
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[client]"
source ~/zdb_deployment/.env
```

Common install options:

```bash
# Client SDK + ETL authoring surface
pip install -e ".[client]"

# Full Python platform workspace
pip install -e ".[all]"

# Service-focused installs
pip install -e ".[gateway]"
pip install -e ".[overseer]"
```

If you prefer the legacy dependency file, `requirements-client.txt` remains
available, but the extras above are now the preferred install path.

## Distribution Model

The current distribution model is:

- clone the repository for full-system deployment, docs, demos, frontend, and Helm assets
- install `app-code/` in editable mode when you want to use the client SDK or CLI locally

The intended user-facing Python surface is:

- `lakehouse_client.etl` for ETL authoring and runtime helpers
- `lakehouse_client.agents` for baseline fleet management and deployment commands
- `lakehouse-client` as the CLI alias

Publishing the package to PyPI or another package index is future work rather
than a required step for the current project workflow.

If you want only the client-facing package surface in your own scripts:

```python
from lakehouse_client import ensure_ray, build_runtime_env
from lakehouse_client.agents import deploy_baseline_fleet, list_fleet_state
from lakehouse_client.etl import BaseTask, DataSource, DataSink, ServiceTask
```

The intended user-facing ETL imports now live behind `lakehouse_client.etl`
rather than raw `etl.*` module paths.

```python
from lakehouse_client.etl import BaseTask, DataSource, DataSink, ensure_ray
```

If you use Prefect against the cluster deployment:

```bash
prefect config set PREFECT_API_URL="http://${NODE_IP}:30420/api"
```

## Main Services

### Gateway

```bash
cd ~/zdb_deployment/app-code
uv run uvicorn gateway.api.main:app --host 0.0.0.0 --port 8000
```

### Overseer

```bash
cd ~/zdb_deployment/app-code
uv run python -m overseer.main
```

### Frontend

```bash
cd ~/zdb_deployment/app-code/frontend
npm install
npm run dev
```

## Agent Lifecycle Commands

The standard deployment management surface now lives behind `etl-agents`.

The same commands are also available through the packaging-oriented alias
`lakehouse-client`.

### List the baseline fleet profile

```bash
cd ~/zdb_deployment/app-code
uv run etl-agents profiles
uv run lakehouse-client profiles
```

### Deploy one named agent

```bash
cd ~/zdb_deployment/app-code
uv run etl-agents deploy SupportAgent --name SupportAgent
uv run lakehouse-client deploy SupportAgent --name SupportAgent
```

### Deploy the baseline fleet

```bash
cd ~/zdb_deployment/app-code
uv run etl-agents deploy-baseline
uv run lakehouse-client deploy-baseline
```

### Delete one deployment intentionally

Use catalog cleanup when you do not want Overseer to bring the deployment back.

```bash
cd ~/zdb_deployment/app-code
uv run etl-agents delete SupportAgent --clean-catalog
uv run lakehouse-client delete SupportAgent --clean-catalog
```

### Delete the baseline fleet

```bash
cd ~/zdb_deployment/app-code
uv run etl-agents delete-baseline --clean-catalog
uv run lakehouse-client delete-baseline --clean-catalog
```

### Inspect merged catalog and runtime state

```bash
cd ~/zdb_deployment/app-code
uv run etl-agents list
uv run lakehouse-client list
```

## Demo And Validation Workflows

Use the consolidated verification scripts from the repo root when you want the standard correctness flow.

### Full verification pass

```bash
cd ~/zdb_deployment
bash ./scripts/verify/run-all.sh
```

### Deploy baseline fleet only

```bash
cd ~/zdb_deployment
bash ./scripts/verify/01-deploy-baseline.sh
```

### Run control-plane smoke checks

```bash
cd ~/zdb_deployment
bash ./scripts/verify/02-control-plane-smoke.sh
```

### Run the self-healing verification only

```bash
cd ~/zdb_deployment
bash ./scripts/verify/03-self-healing.sh
```

### Run focused regression tests

```bash
cd ~/zdb_deployment/app-code
uv run --extra overseer pytest tests/test_agent_control_live_detection.py tests/test_agent_hub_stale_cleanup.py tests/test_agent_manager_cli.py
```

## Workspace Responsibilities

- `etl/`: framework code, deployment utilities, runtime helpers, and agent infrastructure
- `gateway/`: API layer, auth, adapters, and integration surface for the frontend
- `overseer/`: collectors, policies, actuators, and the control loop
- `pipelines/`: Prefect flows and demo workflows
- `scripts/`: operational helpers and smoke-test entrypoints

## Troubleshooting

### Prefect starts a temporary server

```bash
prefect config set PREFECT_API_URL="http://${NODE_IP}:30420/api"
```

### Ray connects to a local instance instead of the cluster

```bash
source ~/zdb_deployment/.env
echo "$RAY_ADDRESS"
```

### `/api/v1/agents` falls back to catalog-only state

Check:

- Ray cluster health
- AgentHub availability inside Ray
- Overseer snapshot output at `/api/v1/system/overseer/snapshots?n=1`

## Read Next

- Framework and API details: [etl/README.md](etl/README.md)
- Control-plane interaction model: [../docs/architecture/compute-gateway-overseer.md](../docs/architecture/compute-gateway-overseer.md)
- Final demo plan reference: [../docs/FINAL_DEMO_PLAN.md](../docs/FINAL_DEMO_PLAN.md)
- Verification script guide: [../scripts/verify/README.md](../scripts/verify/README.md)
