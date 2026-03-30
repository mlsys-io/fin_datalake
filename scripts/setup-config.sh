#!/usr/bin/env bash
# =============================================================================
# Automated ETL Configuration Setup
# =============================================================================
# This script auto-discovers K8s services and generates configuration files.
# Usage: ./setup-config.sh [--env | --configmap | --both]
# =============================================================================

set -euo pipefail

# Get script location and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Output files - located at project root
ENV_FILE="${PROJECT_ROOT}/.env"
CONFIGMAP_FILE="${PROJECT_ROOT}/k8s-config.yaml"

# Default namespaces
NS_DEMO="demo-sources"
NS_MINIO="etl-storage"
NS_TSDB="etl-data"
NS_PREFECT="etl-orchestrate"
NS_COMPUTE="etl-compute"

echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}    ETL Framework - Automated Configuration Setup${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo ""

# 1. Check prerequisites
echo -e "${YELLOW}[1/6] Checking prerequisites...${NC}"
if ! command -v kubectl &> /dev/null; then
    echo -e "${RED}❌ kubectl not found.${NC}"
    exit 1
fi
if ! kubectl cluster-info &> /dev/null; then
    echo -e "${RED}❌ Cannot connect to K8s cluster.${NC}"
    exit 1
fi
echo -e "${GREEN}✅ kubectl connected to cluster${NC}"

# 2. Discover Node IP
echo -e "${YELLOW}[2/6] Discovering Node IP...${NC}"
NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
if [ -z "$NODE_IP" ]; then
    echo -e "${RED}❌ Could not determine Node IP${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Node IP: ${NODE_IP}${NC}"

# 3. Discover Services
echo -e "${YELLOW}[3/6] Discovering services...${NC}"
get_nodeport() {
    local ns=$1; local svc=$2
    kubectl get svc "$svc" -n "$ns" -o jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null || echo ""
}

# Discovery
API_PORT=$(get_nodeport "$NS_DEMO" "demo-api")
GATEWAY_PORT=$(get_nodeport "$NS_COMPUTE" "etl-gateway-svc")
WS_PORT=$(get_nodeport "$NS_DEMO" "demo-websocket")
KAFKA_PORT=$(get_nodeport "$NS_DEMO" "kafka")
STATIC_PORT=$(get_nodeport "$NS_DEMO" "static-server")

PREFECT_PORT=$(get_nodeport "$NS_PREFECT" "prefect-server")
RAY_CLIENT_PORT=$(kubectl get svc etl-ray-head-svc -n "$NS_COMPUTE" -o jsonpath='{.spec.ports[?(@.name=="client")].nodePort}' 2>/dev/null || echo "")
RAY_DASHBOARD_PORT=$(kubectl get svc etl-ray-head-svc -n "$NS_COMPUTE" -o jsonpath='{.spec.ports[?(@.name=="dashboard")].nodePort}' 2>/dev/null || echo "")
TSDB_PORT=$(kubectl get svc tsdb-ha -n "$NS_TSDB" -o jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null || echo "")
RISINGWAVE_PORT=$(get_nodeport "$NS_TSDB" "risingwave")
HIVE_PORT=$(get_nodeport "etl-storage" "hms-hive-metastore")

# 4. Secrets
echo -e "${YELLOW}[4/6] Extracting secrets...${NC}"
MINIO_ACCESS="minioadmin"
MINIO_SECRET="TgICtB3pdkX5JhQS"
MINIO_ENDPOINT="https://luyao-storage-head.ddns.comp.nus.edu.sg:4000"
MINIO_CA_PATH="/mnt/data/minio-certs/minio-storage-head.crt"

TSDB_PASSWORD=""
TSDB_SECRET_NAME="tsdb-pguser-app"
if kubectl get secret "$TSDB_SECRET_NAME" -n "$NS_TSDB" &>/dev/null; then
    TSDB_PASSWORD=$(kubectl get secret "$TSDB_SECRET_NAME" -n "$NS_TSDB" -o jsonpath='{.data.password}' 2>/dev/null | base64 -d || echo "")
fi

# 5. Internal Gateway Security (M2M)
echo -e "${YELLOW}[5/6] Configuring internal security...${NC}"
# Use existing token if present in .env, otherwise generate
if [ -f "$ENV_FILE" ]; then
    INTERNAL_TOKEN=$(grep "GATEWAY_INTERNAL_TOKEN=" "$ENV_FILE" | cut -d'=' -f2 || echo "")
fi
if [ -z "${INTERNAL_TOKEN:-}" ]; then
    INTERNAL_TOKEN=$(openssl rand -hex 16 2>/dev/null || echo "system-sk-$(date +%s)")
fi
echo -e "${GREEN}✅ Internal token configured${NC}"

# 6. Generate .env
echo -e "${YELLOW}[6/6] Generating configuration files...${NC}"
cat > "$ENV_FILE" << EOF
# Auto-generated on $(date)
export GATEWAY_INTERNAL_TOKEN=${INTERNAL_TOKEN}
export GATEWAY_INTERNAL_URL=http://${NODE_IP}:${GATEWAY_PORT:-30801}
export NODE_IP=${NODE_IP}
export AWS_ACCESS_KEY_ID=${MINIO_ACCESS}
export AWS_SECRET_ACCESS_KEY=${MINIO_SECRET}
export AWS_ENDPOINT_URL=${MINIO_ENDPOINT}
export AWS_REGION=us-east-1
export CA_PATH=${MINIO_CA_PATH}
export DELTA_ROOT=s3://delta-lake/bronze
export TSDB_HOST=${NODE_IP}
export TSDB_PORT=${TSDB_PORT:-30543}
export TSDB_USER=app
export TSDB_PASSWORD='${TSDB_PASSWORD}'
export TSDB_DATABASE=app
export KAFKA_BOOTSTRAP_SERVERS=${NODE_IP}:${KAFKA_PORT:-30909}
export API_URL=http://${NODE_IP}:${GATEWAY_PORT:-30801}
export WEBSOCKET_URL=ws://${NODE_IP}:${WS_PORT:-30876}
export STATIC_URL=http://${NODE_IP}:${STATIC_PORT:-30880}
export HIVE_HOST=${NODE_IP}
export HIVE_PORT=${HIVE_PORT:-30983}
export RISINGWAVE_HOST=${NODE_IP}
export RISINGWAVE_PORT=${RISINGWAVE_PORT:-31001}
export PREFECT_API_URL=http://${NODE_IP}:${PREFECT_PORT:-30420}/api
export RAY_ADDRESS=ray://${NODE_IP}:${RAY_CLIENT_PORT:-30282}
export RAY_DASHBOARD_URL=http://${NODE_IP}:${RAY_DASHBOARD_PORT:-30742}
export OVERSEER_REDIS_URL=redis://:redis-lakehouse-pass@redis-master.etl-storage.svc.cluster.local:6379/0
export INPUT_PATH=/mnt/data
EOF

# 6. Generate K8s YAML
cat > "$CONFIGMAP_FILE" << EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: etl-config
  namespace: "${NS_COMPUTE}"
data:
  NODE_IP: "${NODE_IP}"
  KAFKA_BOOTSTRAP_SERVERS: "kafka.${NS_DEMO}.svc:9092"
  DELTA_ROOT: "s3://delta-lake/bronze"
  HIVE_HOST: "hive-metastore.default.svc.cluster.local"
---
apiVersion: v1
kind: Secret
metadata:
  name: etl-secrets
  namespace: "${NS_COMPUTE}"
type: Opaque
stringData:
  AWS_ACCESS_KEY_ID: "${MINIO_ACCESS}"
  AWS_SECRET_ACCESS_KEY: "${MINIO_SECRET}"
  TSDB_PASSWORD: "${TSDB_PASSWORD}"
  GATEWAY_INTERNAL_TOKEN: "${INTERNAL_TOKEN}"
EOF

echo -e "${GREEN}✅ Generated: ${ENV_FILE}${NC}"
echo -e "${GREEN}✅ Generated: ${CONFIGMAP_FILE}${NC}"
echo ""
echo "Usage: source .env && python -m pipelines.demo_pipeline"
