#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

PREFECT_NAMESPACE="prefect"                 


# --- 1. Check for required dependencies ---
echo "INFO: Checking for required dependencies..."
for cmd in helm kubectl; do
    if ! command -v "$cmd" &> /dev/null; then
        echo "ERROR: $cmd could not be found. Please install it and ensure it's in your PATH." >&2
        exit 1
    fi
done
echo "SUCCESS: All dependencies are installed."


# --- 2. Deploy the Prefect server using Helm ---
echo "INFO: Setting up Prefect Helm repository..."
helm repo add prefect https://prefecthq.github.io/prefect-helm 
helm repo update 
echo "SUCCESS: Helm repository is up to date."

echo "INFO: Deploying Prefect server to namespace '${PREFECT_NAMESPACE}'..."
kubectl create namespace "${PREFECT_NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f 
helm install prefect prefect/prefect -n "${PREFECT_NAMESPACE}" -f values.yaml
echo "SUCCESS: Prefect Helm chart installation initiated."


# --- 3. Final Instructions ---
echo "🚀 Prefect Server Deployment Initiated! 🚀"
echo "Access the Prefect UI by port-forwarding the service:"
echo "kubectl port-forward svc/prefect-server -n ${PREFECT_NAMESPACE} 4200:4200"
echo "Then navigate to http://localhost:4200 in your browser."

