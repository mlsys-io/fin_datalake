# Verification Scripts

This directory is the single entry point for correctness checks and demo-readiness validation.

The verification flow is intentionally narrow:

1. deploy the baseline managed fleet
2. confirm gateway and overseer health
3. run the self-healing demo
4. confirm the system returns to baseline

## Prerequisites

- the cluster is up and reachable
- gateway is exposed at `GATEWAY_BASE_URL` or `http://localhost:30801`
- frontend is exposed at `FRONTEND_BASE_URL` or `http://localhost:30800`
- `uv`, `curl`, and the Python client environment are available
- if you want authenticated API checks, export:
  - `GATEWAY_USER`
  - `GATEWAY_PASSWORD`

You can also use a bearer token instead of cookies:

- `GATEWAY_TOKEN`

## Scripts

### Deploy baseline fleet

```bash
bash ./scripts/verify/01-deploy-baseline.sh
```

Runs:

- `uv run etl-agents deploy-baseline`
- `uv run etl-agents list`

Use this when you want the standard 4-agent baseline in place.

### Control-plane smoke check

```bash
bash ./scripts/verify/02-control-plane-smoke.sh
```

Checks:

- gateway `/healthz`
- gateway `/readyz`
- `/api/v1/agents`
- latest Overseer snapshot

It also prints the frontend routes to verify manually:

- `/agents`
- `/overseer`
- `/not-real`

### Self-healing verification

```bash
bash ./scripts/verify/03-self-healing.sh
```

Runs the managed self-healing demo, then prints:

- post-demo `/api/v1/agents`
- post-demo latest Overseer snapshot

Use this to confirm that:

- the victim deployment is recovered with the same name
- cleanup removes the temporary victim
- the system returns to healthy baseline

### Full verification pass

```bash
bash ./scripts/verify/run-all.sh
```

This is the recommended end-to-end correctness pass before a demo.

## Recommended Success Signals

- `/readyz` returns `"ready": true`
- `/api/v1/agents` returns runtime-backed healthy agents
- latest Overseer snapshot shows `agent_control.healthy = true`
- the self-healing demo reports successful recovery
- after cleanup, the fleet returns to the 4 baseline agents

## Relationship To Older Scripts

Older helper scripts remain in the repo for infrastructure setup and one-off operations. The correctness and demo validation path now lives here under `scripts/verify`.
