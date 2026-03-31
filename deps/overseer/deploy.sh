#!/usr/bin/env bash
# =============================================================================
# AI Lakehouse Overseer Deployment Script
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="etl-compute"

echo "🔍 Checking cluster prerequisites..."

# 1. Check for etl-secrets and etl-config in the target namespace
if ! kubectl get secret etl-secrets -n "$NAMESPACE" >/dev/null 2>&1; then
    echo "❌ Error: 'etl-secrets' not found in namespace '$NAMESPACE'."
    echo "   Run 'scripts/setup-config.sh --env && kubectl apply -f k8s-config.yaml' first."
    exit 1
fi
echo "✅ etl-secrets found."

# 2. Apply manifests
echo "🚀 Deploying Overseer to namespace '$NAMESPACE'..."
kubectl apply -f "${SCRIPT_DIR}/overseer-deploy.yaml"

# 3. Wait for readiness
echo "⏳ Waiting for Overseer pod to become ready (timeout: 90s)..."
if kubectl wait --for=condition=available --timeout=90s deployment/etl-overseer -n "$NAMESPACE"; then
    echo "✅ Overseer is now running."
else
    echo "⚠️  Timeout waiting for Overseer. Check logs with:"
    echo "   kubectl logs -n $NAMESPACE -l app=etl-overseer"
    exit 1
fi

echo "====================================================================="
echo "✅ AI Lakehouse Overseer successfully deployed!"
echo "📍 Domain: Autonomic Control (MAPE-K loop)"
echo "📍 Monitoring: Ray, Kafka, Prefect, Delta Lake, Gateway."
echo "📍 State: Redis (DB 1)"
echo "====================================================================="
