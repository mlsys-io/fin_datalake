#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- CONFIG ---
NAMESPACE="etl-data"
RELEASE_NAME="milvus"
CHART_REPO_NAME="milvus"
CHART_REPO_URL="https://zilliztech.github.io/milvus-helm/"
VALUES_FILE="$SCRIPT_DIR/values.yaml"
KUBECONFIG_PATH="${KUBECONFIG:-$HOME/.kube/config}"

echo "Checking prerequisites..."

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

if [ ! -f "$VALUES_FILE" ]; then
    echo "Milvus values file not found at $VALUES_FILE"
    exit 1
fi

echo "Using values file: $VALUES_FILE"

echo "Adding Milvus Helm repo..."
helm repo add "$CHART_REPO_NAME" "$CHART_REPO_URL" >/dev/null
helm repo update >/dev/null

echo "Creating namespace '$NAMESPACE' (if not exists)..."
kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 || kubectl create namespace "$NAMESPACE"

echo "Deploying Milvus via Helm..."
helm upgrade --install "$RELEASE_NAME" "$CHART_REPO_NAME/milvus" \
    --namespace "$NAMESPACE" \
    --create-namespace \
    --reset-values \
    -f "$VALUES_FILE" \
    --wait \
    --timeout 10m

echo "Rendered Helm values in cluster:"
helm get values "$RELEASE_NAME" -n "$NAMESPACE"

echo "Waiting for Milvus workload readiness..."
kubectl wait --for=condition=available --timeout=600s deployment --all -n "$NAMESPACE" || true
kubectl wait --for=jsonpath='{.status.readyReplicas}'=1 --timeout=600s statefulset --all -n "$NAMESPACE" || true
kubectl wait --for=condition=Ready --timeout=600s pod --all -n "$NAMESPACE" || true

echo "Current Milvus resources:"
kubectl get pods -n "$NAMESPACE"
kubectl get svc -n "$NAMESPACE"
kubectl get pvc -n "$NAMESPACE"

if kubectl get pods -n "$NAMESPACE" --no-headers 2>/dev/null | grep -Eq 'Pending|CrashLoopBackOff|Error|ImagePullBackOff|Init:'; then
    echo
    echo "Milvus deployment has non-ready pods. Dumping diagnostics..."
    kubectl get events -n "$NAMESPACE" --sort-by=.lastTimestamp | tail -n 100 || true
    for pod in $(kubectl get pods -n "$NAMESPACE" --no-headers | awk '{print $1}'); do
        echo
        echo "==== describe pod/$pod ===="
        kubectl describe pod "$pod" -n "$NAMESPACE" || true
        echo
        echo "==== logs pod/$pod ===="
        kubectl logs "$pod" -n "$NAMESPACE" --all-containers --tail=200 || true
    done
    exit 1
fi

echo "Milvus deployed successfully."
echo
echo "To check the status:"
echo "  kubectl get pods -n $NAMESPACE"
echo
echo "To port-forward Milvus (default port 19530):"
echo "  kubectl port-forward svc/${RELEASE_NAME}-milvus 19530:19530 -n $NAMESPACE"
echo
echo "To uninstall:"
echo "  helm uninstall $RELEASE_NAME -n $NAMESPACE && kubectl delete namespace $NAMESPACE"
