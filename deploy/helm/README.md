# Helm Deployment

This directory contains a first-party Helm chart for the platform and its
optional bundled dependencies.

The chart currently lives inside this repository and is installed from the
local chart path. It is not yet published to a Helm repository or OCI registry,
so operators clone the repo and run Helm commands against
`deploy/helm/ai-lakehouse-platform`.

## Included services

- `etl-gateway`
- `etl-overseer`
- optional bundled `redis`
- optional bundled `prefect`
- optional bundled `kuberay-operator`
- optional bundled `rayCluster`
- optional bundled `pgo` + `timescaledb`
- optional bundled `milvus`
- optional bundled `risingwave`

The chart supports two operator modes:

- bring your own dependencies: keep the dependency flags disabled and point
  `global.externalServices.*` at your existing infrastructure
- one-go deployment: enable the dependency flags and install the stack into one
  namespace

## Current Distribution

Typical usage today looks like:

```bash
git clone <your-repo>
cd <your-repo>
helm dependency update ./deploy/helm/ai-lakehouse-platform
helm upgrade --install ai-lakehouse ./deploy/helm/ai-lakehouse-platform -n etl-platform --create-namespace
```

If you later publish the chart, these commands can be replaced with a normal
Helm repo or OCI install flow. Until then, the repo itself is the distribution
channel.

## Bring Your Own Install

```bash
helm upgrade --install ai-lakehouse ./deploy/helm/ai-lakehouse-platform \
  --namespace etl-platform \
  --create-namespace \
  --set gateway.secretEnv.stringData.GATEWAY_DATABASE_URL="postgresql+asyncpg://app:password@tsdb-ha.example.svc.cluster.local:5432/app" \
  --set global.externalServices.redisUrl="redis://:password@redis.example.svc.cluster.local:6379/0" \
  --set global.externalServices.prefectHost="prefect.example.svc.cluster.local" \
  --set global.externalServices.prefectUiUrl="http://prefect.example.svc.cluster.local:4200" \
  --set global.externalServices.rayHost="ray-head.example.svc.cluster.local" \
  --set global.externalServices.rayAddress="ray://ray-head.example.svc.cluster.local:10001" \
  --set global.externalServices.rayDashboardUrl="http://ray-head.example.svc.cluster.local:8265" \
  --set global.externalServices.tsdbHost="tsdb-ha.example.svc.cluster.local"
```

## One-Go Install

```bash
helm dependency update ./deploy/helm/ai-lakehouse-platform
helm upgrade --install ai-lakehouse ./deploy/helm/ai-lakehouse-platform \
  --namespace etl-platform \
  --create-namespace \
  --set redis.enabled=true \
  --set prefect.enabled=true \
  --set kuberay-operator.enabled=true \
  --set rayCluster.enabled=true \
  --set pgo.enabled=true \
  --set timescaledb.enabled=true \
  --set gateway.secretEnv.stringData.GATEWAY_DATABASE_URL="postgresql+asyncpg://app:password@tsdb-ha.etl-platform.svc.cluster.local:5432/app"
```

## Notes

- By default the chart reads shared credentials from an existing `etl-secrets`
  secret in the release namespace.
- Gateway supports an inline generated secret for `GATEWAY_DATABASE_URL` so the
  previous shell-based secret extraction step is no longer required when the DB
  URL is already known.
- Optional scoped secrets `etl-user-secret-gateway`,
  `etl-user-secret-overseer`, and `etl-user-secret-ray` are still supported
  through `envFrom`.
- When bundled dependencies are enabled, the chart assumes a single release
  namespace and rewrites the internal service URLs to match that namespace.
- `timescaledb.enabled=true` assumes the bundled Crunchy PGO operator is also
  enabled or already present in the cluster.
