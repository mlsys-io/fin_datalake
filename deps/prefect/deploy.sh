#!/usr/bin/env bash
set -euo pipefail

# --- CONFIG ---
NAMESPACE="etl-orchestrate"
RELEASE_NAME="prefect-server"
CHART_REPO_NAME="prefect"
CHART_REPO_URL="https://prefecthq.github.io/prefect-helm"
VALUES_FILE="values.yaml"  # optional
KUBECONFIG_PATH="${KUBECONFIG:-$HOME/.kube/config}"

echo "🔍 Checking prerequisites..."

if ! command -v helm &> /dev/null; then
    echo "Helm not found. Please install Helm: https://helm.sh/docs/intro/install/"
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

echo "Adding Prefect Helm repo..."
helm repo add "$CHART_REPO_NAME" "$CHART_REPO_URL" >/dev/null
helm repo update >/dev/null

echo "Creating namespace '$NAMESPACE' (if not exists)..."
kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 || kubectl create namespace "$NAMESPACE"

echo "Deploying Prefect Server via Helm..."
if [ -f "$VALUES_FILE" ]; then
    helm upgrade --install "$RELEASE_NAME" "$CHART_REPO_NAME/prefect-server" \
        --namespace "$NAMESPACE" \
        -f "$VALUES_FILE"
else
    helm upgrade --install "$RELEASE_NAME" "$CHART_REPO_NAME/prefect-server" \
        --namespace "$NAMESPACE"
fi

echo "Waiting for Prefect pods to be ready..."
kubectl wait --for=condition=available --timeout=600s deployment --all -n "$NAMESPACE" || true
kubectl get pods -n "$NAMESPACE"

echo "✅ Prefect Server deployed successfully!"
echo
echo "Prefect UI is configured to run behind the Nginx single entry point."
echo "Open it via: http://<node-ip>:30800/prefect/"
echo "To check deployment status:"
echo "  kubectl get pods -n $NAMESPACE"
echo
echo "To uninstall:"
echo "  helm uninstall $RELEASE_NAME -n $NAMESPACE && kubectl delete namespace $NAMESPACE"
