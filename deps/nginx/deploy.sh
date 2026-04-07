#!/usr/bin/env bash
# =============================================================================
# Lakehouse Nginx Ingress - Deployment Orchestrator
# =============================================================================
set -euo pipefail

NAMESPACE="etl-compute"
IMAGE_NAME="nginx-frontend:latest"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FRONTEND_DIR="${SCRIPT_DIR}/../../app-code/frontend"

echo "🚀 Building React Frontend and Nginx Image..."

# Check prerequisites
if ! command -v docker &> /dev/null; then
    echo "❌ Error: docker not found."
    exit 1
fi
if ! command -v k0s &> /dev/null; then
    echo "❌ Error: k0s not found. This script must run on the cluster node."
    exit 1
fi

# Build Docker image (multi-stage Dockerfile handles 'npm run build' inside)
sudo docker build -t "$IMAGE_NAME" "$FRONTEND_DIR"

echo "📦 Importing image to k0s containerd..."
sudo docker save "$IMAGE_NAME" | sudo k0s ctr images import -

echo "☸️ Applying Kubernetes manifests..."
kubectl apply -f "${SCRIPT_DIR}/nginx-configmap.yaml"
kubectl apply -f "${SCRIPT_DIR}/nginx-deploy.yaml"

echo "✅ Deployment complete!"
echo "📍 Access your UI at: http://<node-ip>:30800"
echo "📍 API / Gateway: http://<node-ip>:30800/api/"
echo "📍 Ray Dashboard: http://<node-ip>:30800/ray/"
echo "📍 Prefect UI:    http://<node-ip>:30800/prefect/"
