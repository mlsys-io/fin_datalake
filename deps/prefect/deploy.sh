#!/bin/bash

# --- CONFIGURATION ---
# This section defines names used for the Helm releases and namespaces.
PREFECT_NAMESPACE="prefect"
PREFECT_SERVER_RELEASE_NAME="prefect-server"
PREFECT_WORKER_RELEASE_NAME="prefect-ray-worker"

# --- SCRIPT LOGIC ---
set -e

# 1. Check for required dependencies
for cmd in helm kubectl; do
    if ! command -v "$cmd" &> /dev/null; then
        echo "ERROR: $cmd could not be found. Please install it." >&2
        exit 1
    fi
done

# 2. Set up Helm repository
helm repo add prefect https://prefecthq.github.io/prefect-helm &>/dev/null || true
helm repo update

# 3. Ensure namespace exists
kubectl create namespace "${PREFECT_NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

# 4. Deploy Prefect Server
# This command installs the server without any custom values.
echo "INFO: Deploying Prefect Server..."
helm upgrade --install "${PREFECT_SERVER_RELEASE_NAME}" prefect/prefect-server \
  --namespace "${PREFECT_NAMESPACE}" \
  --wait

# 5. Deploy Prefect Worker
# This command uses the separate 'worker-values.yaml' file to configure the worker.
echo "INFO: Deploying Prefect Worker..."
helm upgrade --install "${PREFECT_WORKER_RELEASE_NAME}" prefect/prefect-worker \
  --namespace "${PREFECT_NAMESPACE}" \
  -f values.yaml

# --- Final Instructions ---
echo ""
echo "🚀 Prefect Server & Worker Deployment Complete! 🚀"
echo ""
echo "To monitor pod status, run:"
echo "kubectl get pods -n ${PREFECT_NAMESPACE} -w"
echo ""
echo "To access the Prefect UI, run this in a new terminal:"
echo "kubectl port-forward svc/${PREFECT_SERVER_RELEASE_NAME} -n ${PREFECT_NAMESPACE} 4200:4200"
echo ""
echo "Then, open your browser and go to http://localhost:4200"

