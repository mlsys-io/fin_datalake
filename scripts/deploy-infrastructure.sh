#!/bin/bash
# =============================================================================
# ETL Cluster Master Deployment Script
# Usage: ./deploy-infrastructure.sh [component|all]
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DEPS_DIR="${PROJECT_ROOT}/deps"
PREFECT_VALUES_FILE="${DEPS_DIR}/prefect/values.yaml"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

# Namespace Config
NS_COMPUTE="etl-compute"
NS_ORCHESTRATE="etl-orchestrate"
NS_STORAGE="etl-storage"
NS_DATA="etl-data"
NS_SOURCES="demo-sources"

log_step()  { echo -e "${BLUE}[STEP]${NC} $1"; }
log_info()  { echo -e "${GREEN}[INFO]${NC} $1"; }

ensure_helm_repo() {
    local name="$1"
    local url="$2"
    if ! helm repo list | awk '{print $1}' | grep -qx "$name"; then
        helm repo add "$name" "$url"
    fi
}

deploy_namespaces() {
    log_step "Creating namespaces..."
    for ns in "$NS_COMPUTE" "$NS_ORCHESTRATE" "$NS_STORAGE" "$NS_DATA" "$NS_SOURCES"; do
        kubectl get namespace "$ns" &>/dev/null || kubectl create namespace "$ns"
    done
}

deploy_kuberay() {
    log_step "Deploying KubeRay..."
    ensure_helm_repo "kuberay" "https://ray-project.github.io/kuberay-helm"
    helm repo update
    helm upgrade --install kuberay-operator kuberay/kuberay-operator -n "$NS_COMPUTE"
}

deploy_prefect() {
    log_step "Deploying Prefect..."
    ensure_helm_repo "prefect" "https://prefecthq.github.io/prefect-helm"
    helm repo update
    helm upgrade --install prefect-server prefect/prefect-server \
        -n "$NS_ORCHESTRATE" \
        --reset-values \
        -f "$PREFECT_VALUES_FILE"
}

deploy_redis() {
    log_step "Deploying Redis..."
    if [ -f "$DEPS_DIR/redis/deploy.sh" ]; then
        bash "$DEPS_DIR/redis/deploy.sh"
    fi
}

deploy_all() {
    deploy_namespaces
    deploy_kuberay
    deploy_prefect
    deploy_redis
    log_info "Infrastructure deployment complete."
}

COMPONENT="${1:-all}"
case "$COMPONENT" in
    all) deploy_all ;;
    namespaces) deploy_namespaces ;;
    kuberay) deploy_kuberay ;;
    prefect) deploy_prefect ;;
    *) echo "Usage: $0 [namespaces|kuberay|prefect|all]"; exit 1 ;;
esac
