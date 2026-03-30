#!/usr/bin/env bash
# =============================================================================
# AI Lakehouse Gateway Deployment Script
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="etl-compute"
IMAGE_NAME="etl-gateway:latest"

echo "🔍 Checking cluster prerequisites..."

# CRITICAL: etl-secrets MUST exist. Exit if missing.
if ! kubectl get secret etl-secrets -n "$NAMESPACE" &> /dev/null; then
    echo "❌ Error: 'etl-secrets' not found in namespace '$NAMESPACE'."
    echo "   Run 'scripts/setup-config.sh --env && kubectl apply -f k8s-config.yaml' first."
    exit 1
fi
echo "✅ etl-secrets found."

# =============================================================================
# NOTE: This script assumes the image has already been imported into k0s containerd.
# If you haven't done so, the pod will fail with ErrImageNeverPull.
# Run: sudo docker save etl-gateway:latest | sudo k0s ctr images import -
# =============================================================================

# Ensure namespace exists
kubectl get namespace "$NAMESPACE" &> /dev/null || kubectl create namespace "$NAMESPACE"

# =============================================================================
# Create etl-gateway-secret in etl-compute namespace.
# K8s does not allow cross-namespace secretKeyRef, so we extract the TSDB
# password from 'etl-data' and create a local secret here.
# =============================================================================
echo "🔑 Extracting database credentials from etl-data namespace..."
TSDB_PASSWORD=$(kubectl get secret tsdb-pguser-app -n etl-data \
    -o jsonpath='{.data.password}' | base64 -d)

DB_URL="postgresql+asyncpg://app:${TSDB_PASSWORD}@tsdb-ha.etl-data.svc.cluster.local:5432/app"

kubectl delete secret etl-gateway-secret -n "$NAMESPACE" 2>/dev/null || true
kubectl create secret generic etl-gateway-secret \
    --namespace "$NAMESPACE" \
    --from-literal=GATEWAY_DATABASE_URL="$DB_URL"
echo "✅ etl-gateway-secret created in '$NAMESPACE'."

kubectl apply -f "${SCRIPT_DIR}/gateway-deploy.yaml"

echo ""
echo "📊 Gateway NodePort: 30801"
echo "⏳ Waiting for Gateway pod to become ready (timeout: 90s)..."
kubectl wait --for=condition=available --timeout=90s deployment/etl-gateway -n "$NAMESPACE"

echo ""
kubectl get pods -n "$NAMESPACE" -l app=etl-gateway
echo ""
echo "✅ Deployment complete!"
echo ""
echo "💡 Check logs for your initial Admin credentials:"
echo "   kubectl logs -n $NAMESPACE -l app=etl-gateway"
