#!/usr/bin/env bash
# =============================================================================
# KubeRay Deployment Script
# Deploys KubeRay operator and Ray cluster to etl-compute namespace
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="etl-compute"

echo "🔍 Checking prerequisites..."
if ! command -v helm &> /dev/null; then
    echo "❌ Helm not found. Please install Helm."
    exit 1
fi

echo "📦 Adding KubeRay Helm repo..."
helm repo add kuberay https://ray-project.github.io/kuberay-helm
helm repo update

echo "🏗️ Creating namespace '$NAMESPACE' (if not exists)..."
kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 || kubectl create namespace "$NAMESPACE"

echo "🚀 Installing KubeRay operator..."
helm upgrade --install kuberay-operator kuberay/kuberay-operator \
    --namespace "$NAMESPACE" \
    --reset-values \
    --set watchNamespace="" \
    --wait

echo "⏳ Waiting for operator to be ready..."
kubectl wait --for=condition=available --timeout=120s deployment/kuberay-operator -n "$NAMESPACE"

echo "🌟 Deploying Ray cluster..."
kubectl apply -f "$SCRIPT_DIR/ray-cluster.yaml"

echo "⏳ Waiting for Ray cluster to start..."
sleep 10
kubectl get pods -n "$NAMESPACE"

echo ""
echo "✅ KubeRay deployment complete!"
echo ""
echo "📊 Ray Dashboard: kubectl port-forward svc/etl-ray-head-svc 8265:8265 -n $NAMESPACE"
echo "🔌 Ray Client: ray://$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}'):10001"
echo ""
echo "📋 Check status:"
echo "   kubectl get rayclusters -n $NAMESPACE"
echo "   kubectl get pods -n $NAMESPACE"
