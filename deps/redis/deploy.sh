#!/bin/bash

# ==============================================================================
# Redis Deployment Script
# Deploys Redis to the Kubernetes cluster using the Bitnami Helm chart
# and the local values.yaml.
# Usage: ./deploy.sh [PASSWORD]
# ==============================================================================

REDIS_PASSWORD=${1:-"redis-lakehouse-pass"}
NAMESPACE="etl-storage"
SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")

echo "Deploying Redis to namespace: $NAMESPACE"

# Ensure namespace exists
kubectl get namespace | grep -q "^$NAMESPACE " || kubectl create namespace $NAMESPACE

# Add Bitnami repo if not exists
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update

# Install Redis referencing the values.yaml in the same directory
helm upgrade --install redis bitnami/redis \
  --namespace $NAMESPACE \
  -f "$SCRIPT_DIR/values.yaml" \
  --set auth.password=$REDIS_PASSWORD

echo "============================================================================="
echo "Redis deployed successfully!"
echo "Architecture: Standalone (In-Memory, No Persistence)"
echo "Password: $REDIS_PASSWORD"
echo ""
echo "Connection String for apps in the same namespace:"
echo "redis://:$REDIS_PASSWORD@redis-master:6379/0"
echo ""
echo "Connection String for apps in diff namespaces (e.g., default):"
echo "redis://:$REDIS_PASSWORD@redis-master.$NAMESPACE.svc.cluster.local:6379/0"
echo "============================================================================="
