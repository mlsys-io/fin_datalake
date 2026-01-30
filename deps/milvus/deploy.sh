#!/usr/bin/env bash
set -euo pipefail

# --- CONFIG ---
NAMESPACE="etl-data"
RELEASE_NAME="milvus"
CHART_REPO_NAME="milvus"
CHART_REPO_URL="https://zilliztech.github.io/milvus-helm/"
VALUES_FILE="values.yaml"  # optional custom Helm values
KUBECONFIG_PATH="${KUBECONFIG:-$HOME/.kube/config}"

echo "🔍 Checking prerequisites..."

if ! command -v helm &> /dev/null; then
    echo "❌ Helm not found. Please install Helm: https://helm.sh/docs/intro/install/"
    exit 1
fi

if ! command -v kubectl &> /dev/null; then
    echo "❌ kubectl not found. Please install kubectl."
    exit 1
fi

if [ ! -f "$KUBECONFIG_PATH" ]; then
    echo "❌ kubeconfig not found at $KUBECONFIG_PATH"
    exit 1
fi

echo "🧭 Adding Milvus Helm repo..."
helm repo add "$CHART_REPO_NAME" "$CHART_REPO_URL" >/dev/null
helm repo update >/dev/null

echo "📦 Creating namespace '$NAMESPACE' (if not exists)..."
kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 || kubectl create namespace "$NAMESPACE"

echo "🚀 Deploying Milvus via Helm..."
if [ -f "$VALUES_FILE" ]; then
    helm upgrade --install "$RELEASE_NAME" "$CHART_REPO_NAME/milvus" \
        --namespace "$NAMESPACE" \
        -f "$VALUES_FILE"
else
    helm upgrade --install "$RELEASE_NAME" "$CHART_REPO_NAME/milvus" \
        --namespace "$NAMESPACE"
fi

echo "⏳ Waiting for Milvus pods to be ready..."
kubectl wait --for=condition=available --timeout=600s deployment --all -n "$NAMESPACE" || true
kubectl get pods -n "$NAMESPACE"

echo "✅ Milvus deployed successfully!"
echo
echo "To check the status:"
echo "  kubectl get pods -n $NAMESPACE"
echo
echo "To port-forward Milvus (default port 19530):"
echo "  kubectl port-forward svc/${RELEASE_NAME}-milvus 19530:19530 -n $NAMESPACE"
echo
echo "To uninstall:"
echo "  helm uninstall $RELEASE_NAME -n $NAMESPACE && kubectl delete namespace $NAMESPACE"
