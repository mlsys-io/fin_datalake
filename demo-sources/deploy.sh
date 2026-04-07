# Demo Sources Deployment Script
# Deploy all demo data sources to Kubernetes
# Usage: ./deploy.sh [apply|delete|status]

# Exit on error
set -e

NAMESPACE="demo-sources"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
echo_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
echo_error() { echo -e "${RED}[ERROR]${NC} $1"; }

apply_manifests() {
    echo_info "Deploying demo sources to namespace: $NAMESPACE"
    
    # 1. Create namespace
    echo_info "Creating namespace..."
    kubectl apply -f "$SCRIPT_DIR/k8s/namespace.yaml"
    
    # 2. Create ConfigMaps from YAML files
    echo_info "Creating ConfigMaps from YAML..."
    kubectl apply -f "$SCRIPT_DIR/k8s/configmaps.yaml"
    
    # 3. Create ConfigMaps from source files (no duplication!)
    echo_info "Creating SQLite ConfigMap from source files..."
    kubectl create configmap sqlite-scripts \
        --namespace=$NAMESPACE \
        --from-file="$SCRIPT_DIR/sqlite/setup_db.py" \
        --from-file="$SCRIPT_DIR/sqlite/serve.py" \
        --dry-run=client -o yaml | kubectl apply -f -
    
    # 4. Deploy services
    echo_info "Deploying WebSocket server..."
    kubectl apply -f "$SCRIPT_DIR/k8s/websocket.yaml"
    
    echo_info "Deploying Kafka..."
    kubectl apply -f "$SCRIPT_DIR/k8s/kafka.yaml"
    
    echo_info "Deploying Static file server..."
    kubectl apply -f "$SCRIPT_DIR/k8s/static-server.yaml"
    
    echo_info "Deploying SQLite server..."
    kubectl apply -f "$SCRIPT_DIR/k8s/sqlite.yaml"
    
    # 5. Build and deploy API (requires Docker)
    echo_warn "Demo API requires building Docker image first:"
    echo "  cd $SCRIPT_DIR/api && docker build -t demo-api:latest ."
    echo "  Then: kubectl apply -f $SCRIPT_DIR/k8s/demo-api.yaml"
    
    # 6. Show status
    echo ""
    echo_info "Waiting for pods to be ready..."
    sleep 5
    kubectl get pods -n "$NAMESPACE"
    
    echo ""
    echo_info "Service endpoints:"
    kubectl get svc -n "$NAMESPACE"
    
    echo ""
    echo_info "Access via NodePort:"
    echo "  - WebSocket: ws://<node-ip>:30876"
    echo "  - Kafka:     <node-ip>:30909"
    echo "  - Static:    http://<node-ip>:30880/data/"
    echo "  - SQLite:    http://<node-ip>:30808"
    echo "  - API:       http://<node-ip>:30800"
}

delete_manifests() {
    echo_warn "Deleting demo sources from namespace: $NAMESPACE"
    
    kubectl delete -f "$SCRIPT_DIR/k8s/demo-api.yaml" --ignore-not-found=true
    kubectl delete -f "$SCRIPT_DIR/k8s/sqlite.yaml" --ignore-not-found=true
    kubectl delete configmap sqlite-scripts -n "$NAMESPACE" --ignore-not-found=true
    kubectl delete -f "$SCRIPT_DIR/k8s/static-server.yaml" --ignore-not-found=true
    kubectl delete -f "$SCRIPT_DIR/k8s/kafka.yaml" --ignore-not-found=true
    kubectl delete -f "$SCRIPT_DIR/k8s/websocket.yaml" --ignore-not-found=true
    kubectl delete -f "$SCRIPT_DIR/k8s/configmaps.yaml" --ignore-not-found=true
    kubectl delete -f "$SCRIPT_DIR/k8s/namespace.yaml" --ignore-not-found=true
    
    echo_info "Demo sources deleted"
}

status() {
    echo_info "Demo sources status:"
    echo ""
    kubectl get pods -n "$NAMESPACE" -o wide 2>/dev/null || echo_warn "Namespace not found"
    echo ""
    kubectl get svc -n "$NAMESPACE" 2>/dev/null || true
}

# Main
case "${1:-apply}" in
    apply)
        apply_manifests
        ;;
    delete|remove)
        delete_manifests
        ;;
    status)
        status
        ;;
    *)
        echo "Usage: $0 [apply|delete|status]"
        exit 1
        ;;
esac
