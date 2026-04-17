#!/usr/bin/env bash
# =============================================================================
# ETL Cluster Cleanup Script
# Usage: ./cleanup-cluster.sh [--dry-run]
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DEPS_DIR="${PROJECT_ROOT}/deps"

NS_COMPUTE="etl-compute"
NS_ORCHESTRATE="etl-orchestrate"
NS_STORAGE="etl-storage"
NS_DATA="etl-data"
NS_SOURCES="demo-sources"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

DRY_RUN=false
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN=true
    echo -e "${YELLOW}[DRY-RUN MODE] No changes will be made${NC}"
fi

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

run_cmd() {
    if $DRY_RUN; then
        printf '%b[DRY-RUN]%b' "${YELLOW}" "${NC}"
        printf ' %q' "$@"
        printf '\n'
        return 0
    fi

    printf '%b[RUN]%b' "${GREEN}" "${NC}"
    printf ' %q' "$@"
    printf '\n'
    "$@" || true
}

echo "=========================================="
echo "  ETL Cluster Cleanup"
echo "=========================================="

log_step "Removing gateway deployment..."
if [[ -x "${DEPS_DIR}/gateway/down.sh" ]]; then
    run_cmd bash "${DEPS_DIR}/gateway/down.sh"
else
    run_cmd kubectl delete -f "${DEPS_DIR}/gateway/gateway-deploy.yaml" --ignore-not-found=true
    run_cmd kubectl delete secret etl-gateway-secret -n "${NS_COMPUTE}" --ignore-not-found=true
fi

log_step "Removing overseer deployment..."
run_cmd kubectl delete deployment etl-overseer -n "${NS_COMPUTE}" --ignore-not-found=true

log_step "Removing Ray cluster resources..."
run_cmd kubectl delete raycluster etl-ray -n "${NS_COMPUTE}" --ignore-not-found=true

log_step "Uninstalling Helm releases..."
run_cmd helm uninstall kuberay-operator -n "${NS_COMPUTE}"
run_cmd helm uninstall prefect-server -n "${NS_ORCHESTRATE}"
run_cmd helm uninstall redis -n "${NS_STORAGE}"
run_cmd helm uninstall hms -n "${NS_STORAGE}"
run_cmd helm uninstall hms-db -n "${NS_STORAGE}"
run_cmd helm uninstall risingwave -n "${NS_DATA}"
run_cmd helm uninstall tsdb -n "${NS_DATA}"

log_step "Deleting namespaces..."
for ns in "${NS_COMPUTE}" "${NS_ORCHESTRATE}" "${NS_STORAGE}" "${NS_DATA}" "${NS_SOURCES}"; do
    run_cmd kubectl delete namespace "$ns" --ignore-not-found=true --wait=false
done

echo "Cleanup complete!"
