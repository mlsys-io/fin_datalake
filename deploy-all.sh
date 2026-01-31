#!/bin/bash
# =============================================================================
# ETL Cluster Master Deployment Script
# Deploys all components with unified etl-* namespace structure
# =============================================================================
# Usage: ./deploy-all.sh [component|all] [--skip-cleanup]
#
# Components:
#   namespaces  - Create all etl-* namespaces
#   kuberay     - KubeRay operator + Ray cluster
#   prefect     - Prefect server + worker
#   hive        - Hive Metastore + PostgreSQL
#   timescaledb - TimescaleDB (Crunchy PGO)
#   risingwave  - RisingWave streaming DB
#   milvus      - Milvus vector DB
#   sources     - Demo data sources (Kafka, API, WebSocket)
#   all         - Deploy everything (default)
#
# Example:
#   ./deploy-all.sh all
#   ./deploy-all.sh prefect
#   ./deploy-all.sh hive --minio-endpoint=https://minio.example.com
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# =============================================================================
# NAMESPACE CONFIGURATION - Edit these to change namespace names
# =============================================================================
NS_COMPUTE="etl-compute"         # Ray cluster
NS_ORCHESTRATE="etl-orchestrate" # Prefect
NS_STORAGE="etl-storage"         # Hive Metastore
NS_DATA="etl-data"               # TimescaleDB, RisingWave, Milvus
NS_SOURCES="demo-sources"         # Demo data sources

# =============================================================================
# Helper Functions
# =============================================================================
log_info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_step()  { echo -e "${BLUE}[STEP]${NC} $1"; }

wait_for_pods() {
    local ns=$1
    local timeout=${2:-120}
    echo "Waiting for pods in $ns to be ready (timeout: ${timeout}s)..."
    kubectl wait --for=condition=ready pod --all -n "$ns" --timeout="${timeout}s" 2>/dev/null || true
}

# =============================================================================
# Deployment Functions
# =============================================================================

deploy_namespaces() {
    log_step "Creating namespaces..."
    for ns in "$NS_COMPUTE" "$NS_ORCHESTRATE" "$NS_STORAGE" "$NS_DATA" "$NS_SOURCES"; do
        kubectl get namespace "$ns" &>/dev/null || kubectl create namespace "$ns"
        log_info "Namespace $ns ready"
    done
}

deploy_kuberay() {
    log_step "Deploying KubeRay operator to $NS_COMPUTE..."
    
    helm repo add kuberay https://ray-project.github.io/kuberay-helm 2>/dev/null || true
    helm repo update
    
    if helm list -n "$NS_COMPUTE" | grep -q kuberay-operator; then
        log_info "KubeRay operator already installed"
    else
        helm install kuberay-operator kuberay/kuberay-operator -n "$NS_COMPUTE"
    fi
    
    log_warn "Ray cluster manifest not auto-deployed. Apply your RayCluster YAML:"
    log_warn "  kubectl apply -f <your-raycluster.yaml> -n $NS_COMPUTE"
}

deploy_prefect() {
    log_step "Deploying Prefect to $NS_ORCHESTRATE..."
    
    helm repo add prefect https://prefecthq.github.io/prefect-helm 2>/dev/null || true
    helm repo update
    
    local values_file="$SCRIPT_DIR/deps/prefect/values.yaml"
    if [ -f "$values_file" ]; then
        helm upgrade --install prefect-server prefect/prefect-server \
            -n "$NS_ORCHESTRATE" -f "$values_file"
    else
        helm upgrade --install prefect-server prefect/prefect-server \
            -n "$NS_ORCHESTRATE"
    fi
    
    wait_for_pods "$NS_ORCHESTRATE"
    log_info "Prefect deployed to $NS_ORCHESTRATE"
}

deploy_hive() {
    log_step "Deploying Hive Metastore to $NS_STORAGE..."
    
    # Check for MinIO credentials
    local minio_endpoint="${MINIO_ENDPOINT:-}"
    local minio_user="${MINIO_USER:-}"
    local minio_password="${MINIO_PASSWORD:-}"
    local minio_cert="${MINIO_CERT:-/mnt/data/minio-certs/minio-storage-head.crt}"
        
    if [ -z "$minio_endpoint" ] || [ -z "$minio_user" ] || [ -z "$minio_password" ]; then
        log_error "Hive requires MinIO credentials. Set environment variables:"
        log_error "  export MINIO_ENDPOINT=https://minio.example.com"
        log_error "  export MINIO_USER=admin"
        log_error "  export MINIO_PASSWORD=secret"
        return 1
    fi
    
    helm repo add bitnami https://charts.bitnami.com/bitnami 2>/dev/null || true
    helm repo update
    
    # Deploy PostgreSQL for HMS
    helm upgrade --install hms-db bitnami/postgresql \
        -n "$NS_STORAGE" -f "$SCRIPT_DIR/deps/hive/hms-db.yaml" || true
    
    # Create MinIO secrets
    kubectl delete configmap minio-ca -n "$NS_STORAGE" 2>/dev/null || true
    kubectl create configmap minio-ca -n "$NS_STORAGE" --from-file=public.crt="$minio_cert" || true
    
    kubectl delete secret minio-creds -n "$NS_STORAGE" 2>/dev/null || true
    kubectl create secret generic minio-creds -n "$NS_STORAGE" \
        --from-literal=username="$minio_user" --from-literal=password="$minio_password"
    
    # Deploy Hive Metastore
    helm upgrade --install hms "$SCRIPT_DIR/deps/hive/hms-chart" \
        -n "$NS_STORAGE" --set s3.endpoint="$minio_endpoint"
    
    wait_for_pods "$NS_STORAGE"
    log_info "Hive Metastore deployed to $NS_STORAGE"
}

deploy_timescaledb() {
    log_step "Deploying TimescaleDB to $NS_DATA..."
    
    # Install Crunchy PGO operator
    if ! helm list -n "$NS_DATA" | grep -q pgo; then
        helm install pgo oci://registry.developers.crunchydata.com/crunchydata/pgo \
            -n "$NS_DATA"
    else
        log_info "PGO operator already installed"
    fi
    
    # Create init SQL configmap
    if ! kubectl -n "$NS_DATA" get configmap tsdb-init-sql &>/dev/null; then
        kubectl -n "$NS_DATA" create configmap tsdb-init-sql \
            --from-file="$SCRIPT_DIR/deps/timescaledb/init.sql"
    fi
    
    # Apply PostgresCluster (need to update namespace in values.yaml)
    local values_file="$SCRIPT_DIR/deps/timescaledb/values.yaml"
    if [ -f "$values_file" ]; then
        # Temporarily patch namespace and apply
        sed "s/namespace: postgres-operator/namespace: $NS_DATA/g" "$values_file" | \
            kubectl apply -n "$NS_DATA" -f -
    fi
    
    log_info "TimescaleDB deployed to $NS_DATA"
    log_info "Connect with: kubectl -n $NS_DATA exec -it tsdb-instance1-XXX-0 -- psql -U postgres -d app"
}

deploy_risingwave() {
    log_step "Deploying RisingWave to $NS_DATA..."
    
    helm repo add risingwavelabs https://risingwavelabs.github.io/helm-charts 2>/dev/null || true
    helm repo update
    
    local values_file="$SCRIPT_DIR/deps/risingwave/risingwave-values.yaml"
    if [ -f "$values_file" ]; then
        helm upgrade --install risingwave risingwavelabs/risingwave \
            -n "$NS_DATA" -f "$values_file"
    else
        helm upgrade --install risingwave risingwavelabs/risingwave \
            -n "$NS_DATA"
    fi
    
    log_info "RisingWave deployed to $NS_DATA"
}

deploy_milvus() {
    log_step "Deploying Milvus to $NS_DATA..."
    
    helm repo add milvus https://zilliztech.github.io/milvus-helm/ 2>/dev/null || true
    helm repo update
    
    local values_file="$SCRIPT_DIR/deps/milvus/values.yaml"
    if [ -f "$values_file" ]; then
        helm upgrade --install milvus milvus/milvus \
            -n "$NS_DATA" -f "$values_file"
    else
        helm upgrade --install milvus milvus/milvus \
            -n "$NS_DATA"
    fi
    
    log_info "Milvus deployed to $NS_DATA"
    log_info "Port-forward: kubectl port-forward svc/milvus-milvus 19530:19530 -n $NS_DATA"
}

deploy_sources() {
    log_step "Deploying demo sources to $NS_SOURCES..."
    
    # Apply namespace
    kubectl apply -f "$SCRIPT_DIR/demo-sources/k8s/namespace.yaml" 2>/dev/null || \
        kubectl create namespace "$NS_SOURCES" 2>/dev/null || true
    
    # Apply ConfigMaps
    kubectl apply -f "$SCRIPT_DIR/demo-sources/k8s/configmaps.yaml" -n "$NS_SOURCES" || true
    
    # Create SQLite scripts ConfigMap
    kubectl create configmap sqlite-scripts \
        --namespace="$NS_SOURCES" \
        --from-file="$SCRIPT_DIR/demo-sources/sqlite/setup_db.py" \
        --from-file="$SCRIPT_DIR/demo-sources/sqlite/serve.py" \
        --dry-run=client -o yaml | kubectl apply -f -
    
    # Deploy services
    kubectl apply -f "$SCRIPT_DIR/demo-sources/k8s/websocket.yaml" -n "$NS_SOURCES" || true
    kubectl apply -f "$SCRIPT_DIR/demo-sources/k8s/kafka.yaml" -n "$NS_SOURCES" || true
    kubectl apply -f "$SCRIPT_DIR/demo-sources/k8s/static-server.yaml" -n "$NS_SOURCES" || true
    kubectl apply -f "$SCRIPT_DIR/demo-sources/k8s/sqlite.yaml" -n "$NS_SOURCES" || true
    
    wait_for_pods "$NS_SOURCES" 60
    
    log_info "Demo sources deployed to $NS_SOURCES"
    log_info "Access:"
    log_info "  - WebSocket: ws://<node-ip>:30876"
    log_info "  - Kafka:     <node-ip>:30909"
    log_info "  - Static:    http://<node-ip>:30880"
    log_info "  - SQLite:    http://<node-ip>:30808"
}

deploy_all() {
    log_step "Deploying all components..."
    
    deploy_namespaces
    deploy_kuberay
    deploy_prefect
    # deploy_hive  # Requires MinIO creds - uncomment when ready
    deploy_timescaledb
    deploy_risingwave
    deploy_milvus
    deploy_sources
    
    echo ""
    log_info "=========================================="
    log_info "  All components deployed!"
    log_info "=========================================="
    echo ""
    kubectl get pods -A | grep "etl-"
}

show_status() {
    echo ""
    log_info "Cluster Status:"
    echo ""
    kubectl get pods -A | grep "etl-\|kube-system" | head -30
    echo ""
    kubectl get svc -A | grep "etl-"
}

# =============================================================================
# Main
# =============================================================================

COMPONENT="${1:-all}"

case "$COMPONENT" in
    namespaces)  deploy_namespaces ;;
    kuberay|ray) deploy_kuberay ;;
    prefect)     deploy_prefect ;;
    hive)        deploy_hive ;;
    timescaledb|tsdb) deploy_timescaledb ;;
    risingwave|rw)    deploy_risingwave ;;
    milvus)      deploy_milvus ;;
    sources|demo) deploy_sources ;;
    all)         deploy_all ;;
    status)      show_status ;;
    *)
        echo "Usage: $0 [component|all|status]"
        echo ""
        echo "Components: namespaces, kuberay, prefect, hive, timescaledb, risingwave, milvus, sources"
        exit 1
        ;;
esac
