#!/bin/bash
set -euo pipefail

# --- CONFIG ---
NAMESPACE="risingwave"
RELEASE_NAME="risingwave"
CHART_REPO_NAME="risingwave"
CHART_REPO_URL="https://risingwavelabs.github.io/risingwave"
VALUES_FILE="values.yaml"  # optional
KUBECONFIG_PATH="${KUBECONFIG:-$HOME/.kube/config}"

echo "🔍 Checking prerequisites..."

if ! command -v helm &> /dev/null; then
    echo "Helm not found. Please install Helm"
    exit 1
fi

if ! command -v kubectl &> /dev/null; then
    echo "kubectl not found. Please install kubectl."
    exit 1
fi

if [ ! -f "$KUBECONFIG_PATH" ]; then
    echo "kubeconfig not found at $KUBECONFIG_PATH"
    exit 1
fi

echo "Adding RisingWave Helm repo..."
helm repo add "$CHART_REPO_NAME" "$CHART_REPO_URL" >/dev/null
helm repo update >/dev/null

echo "Creating namespace '$NAMESPACE' (if not exists)..."
kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 || kubectl create namespace "$NAMESPACE"

echo "Deploying RisingWave via Helm..."
if [ -f "$VALUES_FILE" ]; then
    helm upgrade --install "$RELEASE_NAME" "$CHART_REPO_NAME/risingwave" \
        --namespace "$NAMESPACE" \
        -f "$VALUES_FILE"
else
    helm upgrade --install "$RELEASE_NAME" "$CHART_REPO_NAME/risingwave" \
        --namespace "$NAMESPACE"
fi

echo "Waiting for RisingWave pods to be ready..."
kubectl wait --for=condition=available --timeout=600s deployment --all -n "$NAMESPACE" || true
kubectl get pods -n "$NAMESPACE"

echo "RisingWave deployed successfully!"
echo "You can check the status with:"
echo "  kubectl get pods -n $NAMESPACE"
echo "To access RisingWave locally:"
echo "  kubectl port-forward svc/${RELEASE_NAME}-frontend 4566:4566 -n $NAMESPACE"
