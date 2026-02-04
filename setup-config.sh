#!/usr/bin/env bash
# =============================================================================
# Automated ETL Configuration Setup
# =============================================================================
# This script auto-discovers K8s services and generates configuration files.
# Usage: ./setup-config.sh [--env | --configmap | --both]
#
# Prerequisites:
#   - kubectl configured and connected to cluster
#   - jq installed (for JSON parsing)
# =============================================================================

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Output files
ENV_FILE=".env"
CONFIGMAP_FILE="k8s-config.yaml"

# Default namespaces (adjust if different)
NS_DEMO="demo-sources"
NS_MINIO="etl-storage"
NS_TSDB="etl-data"
NS_PREFECT="etl-orchestrate"
NS_COMPUTE="etl-compute"

echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}    ETL Framework - Automated Configuration Setup${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo ""

# -----------------------------------------------------------------------------
# 1. Check prerequisites
# -----------------------------------------------------------------------------
echo -e "${YELLOW}[1/6] Checking prerequisites...${NC}"

if ! command -v kubectl &> /dev/null; then
    echo -e "${RED}❌ kubectl not found. Please install kubectl first.${NC}"
    exit 1
fi

if ! kubectl cluster-info &> /dev/null; then
    echo -e "${RED}❌ Cannot connect to K8s cluster. Check your kubeconfig.${NC}"
    exit 1
fi

echo -e "${GREEN}✅ kubectl connected to cluster${NC}"

# -----------------------------------------------------------------------------
# 2. Discover Node IP
# -----------------------------------------------------------------------------
echo -e "${YELLOW}[2/6] Discovering Node IP...${NC}"

NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
if [ -z "$NODE_IP" ]; then
    echo -e "${RED}❌ Could not determine Node IP${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Node IP: ${NODE_IP}${NC}"

# -----------------------------------------------------------------------------
# 3. Discover Services & NodePorts
# -----------------------------------------------------------------------------
echo -e "${YELLOW}[3/6] Discovering services...${NC}"

# Function to get NodePort for a service
get_nodeport() {
    local ns=$1
    local svc=$2
    kubectl get svc "$svc" -n "$ns" -o jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null || echo ""
}

# Demo Sources
API_PORT=$(get_nodeport "$NS_DEMO" "demo-api")
WS_PORT=$(get_nodeport "$NS_DEMO" "demo-websocket")
KAFKA_PORT=$(get_nodeport "$NS_DEMO" "kafka")
STATIC_PORT=$(get_nodeport "$NS_DEMO" "static-server")
SQLITE_PORT=$(get_nodeport "$NS_DEMO" "sqlite-server")

echo "  - API Server: ${API_PORT:-not found}"
echo "  - WebSocket: ${WS_PORT:-not found}"
echo "  - Kafka: ${KAFKA_PORT:-not found}"
echo "  - Static Files: ${STATIC_PORT:-not found}"
echo "  - SQLite: ${SQLITE_PORT:-not found}"

# Infrastructure Services
PREFECT_PORT=$(get_nodeport "$NS_PREFECT" "prefect-server")
RAY_CLIENT_PORT=$(kubectl get svc etl-ray-head-svc -n "$NS_COMPUTE" -o jsonpath='{.spec.ports[?(@.name=="client")].nodePort}' 2>/dev/null || echo "")
RAY_DASHBOARD_PORT=$(kubectl get svc etl-ray-head-svc -n "$NS_COMPUTE" -o jsonpath='{.spec.ports[?(@.name=="dashboard")].nodePort}' 2>/dev/null || echo "")
TSDB_PORT=$(kubectl get svc tsdb-ha -n "$NS_TSDB" -o jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null || echo "")
RISINGWAVE_PORT=$(get_nodeport "$NS_TSDB" "risingwave")
HIVE_PORT=$(get_nodeport "etl-storage" "hms-hive-metastore")

echo "  - Prefect: ${PREFECT_PORT:-not found}"
echo "  - Ray Client: ${RAY_CLIENT_PORT:-not found}"
echo "  - Ray Dashboard: ${RAY_DASHBOARD_PORT:-not found}"
echo "  - TimescaleDB: ${TSDB_PORT:-not found}"
echo "  - RisingWave: ${RISINGWAVE_PORT:-not found}"
echo "  - Hive: ${HIVE_PORT:-not found}"

# -----------------------------------------------------------------------------
# 4. Extract Secrets
# -----------------------------------------------------------------------------
echo -e "${YELLOW}[4/6] Extracting secrets...${NC}"

# MinIO credentials (external MinIO - hardcoded since it's outside K8s)
MINIO_ACCESS="minioadmin"
MINIO_SECRET="TgICtB3pdkX5JhQS"
MINIO_ENDPOINT="https://172.28.176.117:4000"
# MinIO TLS certificate path (for HTTPS connections)
MINIO_CA_PATH="/mnt/data/minio-certs/minio-storage-head.crt"
echo "  - MinIO: configured (external)"
echo "  - MinIO CA: ${MINIO_CA_PATH}"

# TimescaleDB password
TSDB_PASSWORD=""
TSDB_SECRET_NAME="tsdb-pguser-app"
if kubectl get secret "$TSDB_SECRET_NAME" -n "$NS_TSDB" &>/dev/null; then
    TSDB_PASSWORD=$(kubectl get secret "$TSDB_SECRET_NAME" -n "$NS_TSDB" -o jsonpath='{.data.password}' 2>/dev/null | base64 -d || echo "")
fi
echo "  - TimescaleDB: ${TSDB_PASSWORD:+found}${TSDB_PASSWORD:-not found}"

# -----------------------------------------------------------------------------
# 5. Generate .env file
# -----------------------------------------------------------------------------
echo -e "${YELLOW}[5/6] Generating configuration files...${NC}"

cat > "$ENV_FILE" << EOF
# =============================================================================
# ETL Framework Configuration
# Auto-generated by setup-config.sh on $(date)
# =============================================================================

# K8s Cluster
export NODE_IP=${NODE_IP}

# MinIO / S3 (Delta Lake Storage) - External MinIO
export AWS_ACCESS_KEY_ID=${MINIO_ACCESS}
export AWS_SECRET_ACCESS_KEY=${MINIO_SECRET}
export AWS_ENDPOINT_URL=${MINIO_ENDPOINT}
export AWS_REGION=us-east-1

# MinIO TLS Certificate (required for HTTPS connections to external MinIO)
export CA_PATH=${MINIO_CA_PATH}
export SSL_CERT_FILE=${MINIO_CA_PATH}

# Delta Lake
export DELTA_ROOT=s3://delta-lake/bronze

# TimescaleDB
export TSDB_HOST=${NODE_IP}
export TSDB_PORT=${TSDB_PORT:-30543}
export TSDB_USER=app
export TSDB_PASSWORD='${TSDB_PASSWORD}'
export TSDB_DATABASE=app

# Kafka
export KAFKA_BOOTSTRAP_SERVERS=${NODE_IP}:${KAFKA_PORT:-30909}

# Demo Sources
export API_URL=http://${NODE_IP}:${API_PORT:-30800}
export WEBSOCKET_URL=ws://${NODE_IP}:${WS_PORT:-30876}
export STATIC_URL=http://${NODE_IP}:${STATIC_PORT:-30880}

# Hive Metastore
export HIVE_HOST=${NODE_IP}
export HIVE_PORT=${HIVE_PORT:-30983}

# RisingWave
export RISINGWAVE_HOST=${NODE_IP}
export RISINGWAVE_PORT=${RISINGWAVE_PORT:-31001}

# Prefect
export PREFECT_API_URL=http://${NODE_IP}:${PREFECT_PORT:-30420}/api

# Ray
export RAY_ADDRESS=ray://${NODE_IP}:${RAY_CLIENT_PORT:-30282}
export RAY_DASHBOARD_URL=http://${NODE_IP}:${RAY_DASHBOARD_PORT:-30742}

# Data Paths
export INPUT_PATH=/mnt/data
EOF

echo -e "${GREEN}✅ Generated: ${ENV_FILE}${NC}"

# -----------------------------------------------------------------------------
# 6. Generate K8s ConfigMap + Secret
# -----------------------------------------------------------------------------
cat > "$CONFIGMAP_FILE" << EOF
# Auto-generated by setup-config.sh on $(date)
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: etl-config
  labels:
    app: etl-framework
data:
  NODE_IP: "${NODE_IP}"
  KAFKA_BOOTSTRAP_SERVERS: "kafka.${NS_DEMO}.svc:9092"
  DELTA_ROOT: "s3://delta-lake/bronze"
  HIVE_HOST: "hive-metastore.default.svc.cluster.local"
  HIVE_PORT: "9083"
  API_URL: "http://demo-api.${NS_DEMO}.svc:8000"
  WEBSOCKET_URL: "ws://websocket-server.${NS_DEMO}.svc:8765"
  CA_PATH: "/opt/certs/public.crt"
  SSL_CERT_FILE: "/opt/certs/public.crt"
---
apiVersion: v1
kind: Secret
metadata:
  name: etl-secrets
  labels:
    app: etl-framework
type: Opaque
stringData:
  AWS_ACCESS_KEY_ID: "${MINIO_ACCESS}"
  AWS_SECRET_ACCESS_KEY: "${MINIO_SECRET}"
  TSDB_PASSWORD: "${TSDB_PASSWORD}"
EOF

echo -e "${GREEN}✅ Generated: ${CONFIGMAP_FILE}${NC}"

# -----------------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------------
echo ""
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ Configuration complete!${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo ""
echo "Generated files:"
echo "  📄 ${ENV_FILE} - For local development (source .env)"
echo "  📄 ${CONFIGMAP_FILE} - For K8s deployment (kubectl apply -f)"
echo ""
echo "Usage:"
echo "  # Local development"
echo "  source .env && python -m pipelines.demo_pipeline"
echo ""
echo "  # K8s deployment"
echo "  kubectl apply -f ${CONFIGMAP_FILE}"
echo ""

# Check for missing values
MISSING=""
[ -z "$MINIO_ACCESS" ] && MISSING="${MISSING}\n  - MinIO credentials"
[ -z "$TSDB_PASSWORD" ] && MISSING="${MISSING}\n  - TimescaleDB password"
[ -z "$API_PORT" ] && MISSING="${MISSING}\n  - Demo API service"

if [ -n "$MISSING" ]; then
    echo -e "${YELLOW}⚠️  Some values could not be auto-discovered:${MISSING}${NC}"
    echo -e "${YELLOW}   You may need to fill these in manually.${NC}"
fi
