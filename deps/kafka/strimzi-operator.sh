#!/bin/bash

set -e
set -o pipefail

NAMESPACE="kafka"
HELM_RELEASE_NAME="strimzi-cluster-operator"
HELM_CHART_OCI_PATH="oci://quay.io/strimzi-helm/strimzi-kafka-operator"

if ! command -v kubectl &> /dev/null; then
    echo "Error: kubectl command not found. Please install kubectl."
    exit 1
fi

if ! command -v helm &> /dev/null; then
    echo "Error: helm command not found. Please install helm."
    exit 1
fi

if ! kubectl cluster-info &> /dev/null; then
    echo "Error: Cannot connect to Kubernetes cluster. Check your kubeconfig."
    exit 1
fi

echo "Ensuring namespace '${NAMESPACE}' exists..."
if ! kubectl get namespace "$NAMESPACE" --ignore-not-found -o name | grep -q "$NAMESPACE"; then
    kubectl create namespace "$NAMESPACE"
    echo "Namespace '${NAMESPACE}' created."
else
    echo "Namespace '${NAMESPACE}' already exists."
fi

echo "Installing Strimzi Operator using Helm chart from ${HELM_CHART_OCI_PATH}..."

helm upgrade --install "$HELM_RELEASE_NAME" "$HELM_CHART_OCI_PATH" \
  --namespace "$NAMESPACE" \
  --set watchAnyNamespace=true \
  --wait

if ! kubectl wait deployment --namespace "$NAMESPACE" "$HELM_RELEASE_NAME" --for condition=Available=True --timeout=300s; then
    echo "Error: Strimzi Operator deployment did not become ready within the timeout period."
    echo "Check the operator pod logs for details:"
    echo "  kubectl logs deployment/${HELM_RELEASE_NAME} -n ${NAMESPACE}"
    exit 1
fi

kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/instance="$HELM_RELEASE_NAME"

echo "Strimzi kafka operator setup completed"

exit 0