#!/bin/bash
set -euo pipefail

# Navigate to script directory for relative paths
cd "$(dirname "$0")"

# Install PGO operator (skip if already exists)
if ! helm list -n etl-data | grep -q pgo; then
  echo "Installing Crunchy PGO operator..."
  helm install pgo oci://registry.developers.crunchydata.com/crunchydata/pgo \
    -n etl-data --create-namespace
else
  echo "PGO operator already installed, skipping..."
fi

# Create configmap (skip if exists)
if ! kubectl -n etl-data get configmap tsdb-init-sql &>/dev/null; then
  echo "Creating init SQL configmap..."
  kubectl -n etl-data create configmap tsdb-init-sql --from-file=init.sql
else
  echo "ConfigMap tsdb-init-sql already exists, skipping..."
fi

# Apply PostgresCluster
echo "Applying PostgresCluster..."
kubectl -n etl-data apply -f values.yaml

# Wait and show status
echo "Checking pods..."
kubectl -n etl-data get pods -l postgres-operator.crunchydata.com/cluster=tsdb

# Copy secret to default namespace (for Ray jobs)
echo "Copying secret to default namespace..."
kubectl get secret tsdb-pguser-app -n etl-data -o json \
  | jq 'del(.metadata.namespace,.metadata.uid,.metadata.resourceVersion,.metadata.creationTimestamp,.metadata.annotations,.metadata.ownerReferences)' \
  | kubectl apply -n default -f -

echo "✅ TimescaleDB setup complete!"
echo ""
echo "Connect with:"
echo "  kubectl -n etl-data exec -it tsdb-instance1-XXX-0 -- psql -U app -d app"