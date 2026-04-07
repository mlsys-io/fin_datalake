#!/bin/bash
# =============================================================================
# ETL Cluster Cleanup Script
# Usage: ./cleanup-cluster.sh [--dry-run]
# =============================================================================

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

DRY_RUN=false
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN=true
    echo -e "${YELLOW}[DRY-RUN MODE] No changes will be made${NC}"
fi

run_cmd() {
    if $DRY_RUN; then
        echo -e "${YELLOW}[DRY-RUN]${NC} $*"
    else
        echo -e "${GREEN}[RUN]${NC} $*"
        eval "$@" || true
    fi
}

echo "=========================================="
echo "  ETL Cluster Cleanup"
echo "=========================================="

# Phase 1: Ray jobs
log_step "Cleaning up completed Ray jobs..."
run_cmd "kubectl delete pod -l app.kubernetes.io/name=kuberay -n default --ignore-not-found=true"

# Phase 2: Helm uninstalls
log_step "Uninstalling Helm releases..."
run_cmd "helm uninstall kuberay-operator -n etl-compute 2>/dev/null || true"
run_cmd "helm uninstall prefect-server -n etl-orchestrate 2>/dev/null || true"
run_cmd "helm uninstall hms -n etl-storage 2>/dev/null || true"
run_cmd "helm uninstall hms-db -n etl-storage 2>/dev/null || true"
run_cmd "helm uninstall risingwave -n etl-data 2>/dev/null || true"
run_cmd "helm uninstall tsdb -n etl-data 2>/dev/null || true"

# Phase 3: Namespaces
log_step "Deleting namespaces..."
OLD_NAMESPACES=("etl-compute" "etl-orchestrate" "etl-storage" "etl-data" "demo-sources")
for ns in "${OLD_NAMESPACES[@]}"; do
    run_cmd "kubectl delete namespace $ns --ignore-not-found=true --wait=false"
done

echo "Cleanup complete!"
