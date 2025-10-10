#!/bin/bash
set -euo pipefail

# === RisingWave Helm deployment (bundled with MinIO + PostgreSQL) ===

NAMESPACE="risingwave"
RELEASE_NAME="risingwave"
VALUES_FILE="risingwave-values.yaml"

echo "🚀 Starting RisingWave bundled deployment..."

# --- Check for Helm ---
if ! command -v helm &> /dev/null; then
  echo "Helm not found. Installing Helm..."
  curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
fi

# --- Create namespace ---
if ! kubectl get namespace "$NAMESPACE" &> /dev/null; then
  echo "Creating namespace: $NAMESPACE"
  kubectl create namespace "$NAMESPACE"
fi

# --- Add RisingWave repo ---
echo "Adding RisingWave Helm repo..."
helm repo add risingwavelabs https://risingwavelabs.github.io/helm-charts
helm repo update

#--- Deploy RisingWave bundle ---
echo "Installing RisingWave bundle with PostgreSQL + MinIO..."
helm install -n risingwave --create-namespace --set tags.bundle=true --set wait=true $RELEASE_NAME risingwavelabs/risingwave

echo "✅ RisingWave bundle deployed successfully!"

# --- Show status ---
kubectl get pods -n "$NAMESPACE"
kubectl get svc -n "$NAMESPACE"

echo ""
echo "🌐 Access RisingWave frontend via NodePort 31001"
echo "🌐 Access RisingWave dashboard via NodePort 31002"
echo ""
echo "You can check logs with:"
echo "  kubectl logs -n $NAMESPACE <pod-name>"
