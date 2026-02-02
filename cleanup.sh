#!/bin/bash
# =============================================================================
# ETL Cluster Cleanup Script
# Removes all existing ETL components before namespace reorganization
# Usage: ./cleanup.sh [--dry-run]
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
echo ""

# --- Phase 1: Delete completed Ray jobs ---
echo -e "${GREEN}[1/4]${NC} Cleaning up completed Ray jobs..."
run_cmd "kubectl delete pod ray-etl-cbjcp ray-summarize-7vc6l -n default --ignore-not-found=true"

# --- Phase 2: Uninstall Helm releases ---
echo ""
echo -e "${GREEN}[2/4]${NC} Uninstalling Helm releases..."

# KubeRay operator
run_cmd "helm uninstall kuberay-operator -n default 2>/dev/null || true"

# Prefect
run_cmd "helm uninstall prefect-server -n prefect 2>/dev/null || true"
run_cmd "helm uninstall prefect-worker -n prefect 2>/dev/null || true"

# RisingWave
run_cmd "helm uninstall risingwave -n risingwave 2>/dev/null || true"

# Hive Metastore
run_cmd "helm uninstall hms -n hive 2>/dev/null || true"
run_cmd "helm uninstall hms-db -n hive 2>/dev/null || true"

# TimescaleDB (Crunchy PGO)
run_cmd "kubectl delete postgrescluster tsdb -n postgres-operator --ignore-not-found=true"
run_cmd "helm uninstall pgo -n postgres-operator 2>/dev/null || true"

# Milvus (if exists)
run_cmd "helm uninstall milvus -n milvus 2>/dev/null || true"

# --- Phase 3: Delete old namespaces ---
echo ""
echo -e "${GREEN}[3/4]${NC} Deleting old namespaces..."

OLD_NAMESPACES=(
    "ray"
    "prefect"
    "risingwave"
    "hive"
    "postgres-operator"
    "milvus"
)

for ns in "${OLD_NAMESPACES[@]}"; do
    run_cmd "kubectl delete namespace $ns --ignore-not-found=true --wait=false"
done

# --- Phase 4: Wait for cleanup ---
echo ""
echo -e "${GREEN}[4/4]${NC} Waiting for namespaces to terminate..."

if ! $DRY_RUN; then
    echo "Waiting up to 60 seconds for namespace cleanup..."
    for i in {1..12}; do
        REMAINING=$(kubectl get namespaces -o name 2>/dev/null | grep -E "^namespace/(ray|prefect|risingwave|hive|postgres-operator|milvus|demo-sources)$" | wc -l)
        if [[ "$REMAINING" -eq 0 ]]; then
            echo -e "${GREEN}All old namespaces deleted!${NC}"
            break
        fi
        echo "  Still cleaning up ($REMAINING namespaces remaining)..."
        sleep 5
    done
fi

# --- Summary ---
echo ""
echo "=========================================="
echo "  Cleanup Complete!"
echo "=========================================="
echo ""
echo "Current namespaces:"
kubectl get namespaces | grep -v "kube-\|default\|local-path"
echo ""
echo "Current pods:"
kubectl get pods -A | grep -v "kube-\|local-path" || echo "No pods found"
echo ""
echo -e "${GREEN}Ready to run deployment scripts with new namespaces.${NC}"
