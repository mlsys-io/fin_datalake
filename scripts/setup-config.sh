#!/usr/bin/env bash
# =============================================================================
# Automated ETL Configuration Setup
# =============================================================================
# This script discovers current cluster services and generates:
#   - .env for local workflows
#   - k8s-config.yaml for cluster deployments
#
# Usage: ./setup-config.sh [--env | --configmap | --both]
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

ENV_FILE="${PROJECT_ROOT}/.env"
CONFIGMAP_FILE="${PROJECT_ROOT}/k8s-config.yaml"

USER_ENV_FILE="${PROJECT_ROOT}/.env.user"

NS_DEMO="demo-sources"
NS_STORAGE="etl-storage"
NS_TSDB="etl-data"
NS_PREFECT="etl-orchestrate"
NS_COMPUTE="etl-compute"

GATEWAY_SERVICE_NAME="etl-gateway-svc"
PREFECT_SERVICE_NAME="prefect-server"
RAY_SERVICE_NAME="etl-ray-head-svc"
TSDB_SERVICE_NAME="tsdb-ha"
RISINGWAVE_SERVICE_NAME="risingwave"
HIVE_SERVICE_NAME="hms-hms"

DEFAULT_MINIO_URL="https://luyao-storage-head.ddns.comp.nus.edu.sg:4000"
MODE="${1:---both}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

usage() {
    cat <<EOF
Usage: ./setup-config.sh [--env | --configmap | --both]

  --env         Generate .env only
  --configmap   Generate k8s-config.yaml only
  --both        Generate both files (default)
EOF
}

quote_shell() {
    local value="$1"
    value="${value//\'/\'\"\'\"\'}"
    printf "'%s'" "$value"
}

yaml_quote() {
    local value="$1"
    value="${value//\\/\\\\}"
    value="${value//\"/\\\"}"
    printf '"%s"' "$value"
}

write_env_var() {
    local key="$1"
    local value="$2"
    printf 'export %s=%s\n' "$key" "$(quote_shell "$value")" >> "$ENV_FILE"
}

write_yaml_kv() {
    local key="$1"
    local value="$2"
    printf '  %s: %s\n' "$key" "$(yaml_quote "$value")" >> "$CONFIGMAP_FILE"
}

load_env_file() {
    local file_path="$1"
    local label="$2"

    if [[ -f "$file_path" ]]; then
        set +u
        set -a
        # shellcheck disable=SC1090
        source "$file_path"
        set +a
        set -u
        echo "  Loaded ${label}"
    fi
}

discover_node_ip() {
    kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' 2>/dev/null || true
}

discover_nodeport() {
    local namespace="$1"
    local service="$2"
    local port_name="${3:-}"
    local default_port="${4:-}"
    local node_port=""

    if [[ -n "$port_name" ]]; then
        node_port="$(kubectl get svc "$service" -n "$namespace" -o jsonpath="{.spec.ports[?(@.name==\"$port_name\")].nodePort}" 2>/dev/null || true)"
    else
        node_port="$(kubectl get svc "$service" -n "$namespace" -o jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null || true)"
    fi

    if [[ -z "$node_port" ]]; then
        node_port="$default_port"
    fi

    printf '%s' "$node_port"
}

ensure_kubectl() {
    if ! command -v kubectl >/dev/null 2>&1; then
        echo -e "${RED}kubectl not found.${NC}"
        exit 1
    fi

    if ! kubectl cluster-info >/dev/null 2>&1; then
        echo -e "${RED}Cannot connect to the Kubernetes cluster.${NC}"
        exit 1
    fi
}

case "$MODE" in
    --env|--configmap|--both)
        ;;
    --help|-h)
        usage
        exit 0
        ;;
    *)
        echo -e "${RED}Unknown option: $MODE${NC}"
        usage
        exit 1
        ;;
esac

echo -e "${BLUE}================================================================${NC}"
echo -e "${BLUE}    ETL Framework - Automated Configuration Setup${NC}"
echo -e "${BLUE}================================================================${NC}"
echo ""

ensure_kubectl

echo -e "${YELLOW}[1/4] Discovering cluster state...${NC}"
NODE_IP="$(discover_node_ip)"
if [[ -z "$NODE_IP" ]]; then
    echo -e "${RED}Could not determine Node IP.${NC}"
    exit 1
fi
echo -e "${GREEN}Node IP: ${NODE_IP}${NC}"

GATEWAY_PORT="$(discover_nodeport "$NS_COMPUTE" "$GATEWAY_SERVICE_NAME" "" 30801)"
PREFECT_PORT="$(discover_nodeport "$NS_PREFECT" "$PREFECT_SERVICE_NAME" "" 30420)"
RAY_CLIENT_PORT="$(discover_nodeport "$NS_COMPUTE" "$RAY_SERVICE_NAME" "client" 30282)"
RAY_DASHBOARD_PORT="$(discover_nodeport "$NS_COMPUTE" "$RAY_SERVICE_NAME" "dashboard" 30742)"
TSDB_PORT="$(discover_nodeport "$NS_TSDB" "$TSDB_SERVICE_NAME" "" 30543)"
RISINGWAVE_PORT="$(discover_nodeport "$NS_TSDB" "$RISINGWAVE_SERVICE_NAME" "" 31001)"
HIVE_PORT="$(discover_nodeport "$NS_STORAGE" "$HIVE_SERVICE_NAME" "" 30983)"

API_PORT="$(discover_nodeport "$NS_DEMO" "demo-api" "" 30800)"
WS_PORT="$(discover_nodeport "$NS_DEMO" "demo-websocket" "" 30876)"
STATIC_PORT="$(discover_nodeport "$NS_DEMO" "static-server" "" 30880)"
KAFKA_PORT="$(discover_nodeport "$NS_DEMO" "kafka" "" 30909)"

echo -e "${GREEN}Gateway NodePort: ${GATEWAY_PORT}${NC}"
echo -e "${GREEN}Prefect NodePort: ${PREFECT_PORT}${NC}"
echo -e "${GREEN}Ray client NodePort: ${RAY_CLIENT_PORT}${NC}"
echo -e "${GREEN}Ray dashboard NodePort: ${RAY_DASHBOARD_PORT}${NC}"
echo -e "${GREEN}TSDB NodePort: ${TSDB_PORT}${NC}"
echo -e "${GREEN}RisingWave NodePort: ${RISINGWAVE_PORT}${NC}"
echo -e "${GREEN}Hive NodePort: ${HIVE_PORT}${NC}"

TSDB_PASSWORD=""
if kubectl get secret tsdb-pguser-app -n "$NS_TSDB" >/dev/null 2>&1; then
    TSDB_PASSWORD="$(kubectl get secret tsdb-pguser-app -n "$NS_TSDB" -o jsonpath='{.data.password}' 2>/dev/null | base64 -d || true)"
fi

echo -e "${YELLOW}[2/4] Loading existing local overrides...${NC}"
if [[ -f "$ENV_FILE" ]]; then
    load_env_file "$ENV_FILE" ".env"
fi

GATEWAY_INTERNAL_TOKEN="${GATEWAY_INTERNAL_TOKEN:-}"
GATEWAY_JWT_SECRET="${GATEWAY_JWT_SECRET:-}"

if [[ -z "$GATEWAY_INTERNAL_TOKEN" ]]; then
    GATEWAY_INTERNAL_TOKEN="$(openssl rand -hex 16 2>/dev/null || echo "system-sk-$(date +%s)")"
fi

if [[ -z "$GATEWAY_JWT_SECRET" ]]; then
    GATEWAY_JWT_SECRET="$(openssl rand -hex 32 2>/dev/null || echo "jwt-secret-$(date +%s)")"
fi

AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-TgICtB3pdkX5JhQS}"
MINIO_ENDPOINT_URL="${MINIO_ENDPOINT_URL:-$DEFAULT_MINIO_URL}"
MINIO_CONSOLE_URL="${MINIO_CONSOLE_URL:-$MINIO_ENDPOINT_URL}"
AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-$MINIO_ENDPOINT_URL}"
AWS_REGION="${AWS_REGION:-us-east-1}"
CA_PATH="${CA_PATH:-/mnt/data/minio-certs/minio-storage-head.crt}"
DELTA_ROOT="${DELTA_ROOT:-s3://delta-lake/bronze}"
OVERSEER_REDIS_URL="${OVERSEER_REDIS_URL:-redis://:redis-lakehouse-pass@redis-master.etl-storage.svc.cluster.local:6379/0}"
INPUT_PATH="${INPUT_PATH:-/mnt/data}"

echo -e "${YELLOW}[3/4] Writing .env (local workflow values)...${NC}"
if [[ "$MODE" == "--env" || "$MODE" == "--both" ]]; then
    cat > "$ENV_FILE" <<EOF
# Auto-generated on $(date)
EOF

    write_env_var GATEWAY_INTERNAL_TOKEN "$GATEWAY_INTERNAL_TOKEN"
    write_env_var GATEWAY_JWT_SECRET "$GATEWAY_JWT_SECRET"
    write_env_var NODE_IP "$NODE_IP"
    write_env_var AWS_ACCESS_KEY_ID "$AWS_ACCESS_KEY_ID"
    write_env_var AWS_SECRET_ACCESS_KEY "$AWS_SECRET_ACCESS_KEY"
    write_env_var AWS_ENDPOINT_URL "$AWS_ENDPOINT_URL"
    write_env_var MINIO_ENDPOINT "$MINIO_ENDPOINT_URL"
    write_env_var MINIO_CONSOLE_URL "$MINIO_CONSOLE_URL"
    write_env_var AWS_REGION "$AWS_REGION"
    write_env_var CA_PATH "$CA_PATH"
    write_env_var DELTA_ROOT "$DELTA_ROOT"
    write_env_var TSDB_HOST "$NODE_IP"
    write_env_var TSDB_PORT "$TSDB_PORT"
    write_env_var TSDB_USER "app"
    write_env_var TSDB_PASSWORD "$TSDB_PASSWORD"
    write_env_var TSDB_DATABASE "app"
    write_env_var KAFKA_BOOTSTRAP_SERVERS "${NODE_IP}:${KAFKA_PORT}"
    write_env_var API_URL "http://${NODE_IP}:${API_PORT}"
    write_env_var WEBSOCKET_URL "ws://${NODE_IP}:${WS_PORT}"
    write_env_var STATIC_URL "http://${NODE_IP}:${STATIC_PORT}"
    write_env_var HIVE_HOST "$NODE_IP"
    write_env_var HIVE_PORT "$HIVE_PORT"
    write_env_var RISINGWAVE_HOST "$NODE_IP"
    write_env_var RISINGWAVE_PORT "$RISINGWAVE_PORT"
    write_env_var PREFECT_API_URL "http://${NODE_IP}:${PREFECT_PORT}/api"
    write_env_var RAY_ADDRESS "ray://${NODE_IP}:${RAY_CLIENT_PORT}"
    write_env_var RAY_NAMESPACE "serve"
    write_env_var RAY_DASHBOARD_URL "http://${NODE_IP}:${RAY_DASHBOARD_PORT}"
    write_env_var OVERSEER_REDIS_URL "$OVERSEER_REDIS_URL"
    write_env_var INPUT_PATH "$INPUT_PATH"

    echo -e "${GREEN}Generated ${ENV_FILE}${NC}"
fi

echo -e "${YELLOW}[4/4] Writing k8s-config.yaml (cluster values)...${NC}"
if [[ "$MODE" == "--configmap" || "$MODE" == "--both" ]]; then
    cat > "$CONFIGMAP_FILE" <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: etl-config
  namespace: "${NS_COMPUTE}"
data:
EOF

    write_yaml_kv NODE_IP "$NODE_IP"
    write_yaml_kv GATEWAY_INTERNAL_URL "http://${GATEWAY_SERVICE_NAME}.${NS_COMPUTE}.svc.cluster.local:8000"
    write_yaml_kv API_URL "http://demo-api.${NS_DEMO}.svc.cluster.local:8000"
    write_yaml_kv WEBSOCKET_URL "ws://demo-websocket.${NS_DEMO}.svc.cluster.local:8765"
    write_yaml_kv STATIC_URL "http://static-server.${NS_DEMO}.svc.cluster.local:80"
    write_yaml_kv KAFKA_BOOTSTRAP_SERVERS "kafka.${NS_DEMO}.svc.cluster.local:9092"
    write_yaml_kv DELTA_ROOT "s3://delta-lake/bronze"
    write_yaml_kv HIVE_HOST "${HIVE_SERVICE_NAME}.${NS_STORAGE}.svc.cluster.local"
    write_yaml_kv HIVE_PORT "9083"
    write_yaml_kv TSDB_HOST "${TSDB_SERVICE_NAME}.${NS_TSDB}.svc.cluster.local"
    write_yaml_kv TSDB_PORT "5432"
    write_yaml_kv TSDB_USER "app"
    write_yaml_kv TSDB_DATABASE "app"
    write_yaml_kv RISINGWAVE_HOST "${RISINGWAVE_SERVICE_NAME}.${NS_TSDB}.svc.cluster.local"
    write_yaml_kv RISINGWAVE_PORT "4567"
    write_yaml_kv RISINGWAVE_USER "root"
    write_yaml_kv RISINGWAVE_DATABASE "dev"
    write_yaml_kv RISINGWAVE_SCHEMA "public"
    write_yaml_kv PREFECT_API_URL "http://${PREFECT_SERVICE_NAME}.${NS_PREFECT}.svc.cluster.local:4200/api"
    write_yaml_kv RAY_ADDRESS "ray://${RAY_SERVICE_NAME}.${NS_COMPUTE}.svc.cluster.local:10001"
    write_yaml_kv RAY_NAMESPACE "serve"
    write_yaml_kv RAY_DASHBOARD_URL "http://${RAY_SERVICE_NAME}.${NS_COMPUTE}.svc.cluster.local:8265"
    write_yaml_kv AWS_ENDPOINT_URL "$AWS_ENDPOINT_URL"
    write_yaml_kv MINIO_ENDPOINT "$MINIO_ENDPOINT_URL"
    write_yaml_kv MINIO_CONSOLE_URL "$MINIO_CONSOLE_URL"
    write_yaml_kv AWS_REGION "$AWS_REGION"
    write_yaml_kv CA_PATH "$CA_PATH"
    write_yaml_kv OVERSEER_REDIS_URL "$OVERSEER_REDIS_URL"
    write_yaml_kv INPUT_PATH "$INPUT_PATH"

    cat >> "$CONFIGMAP_FILE" <<EOF
---
apiVersion: v1
kind: Secret
metadata:
  name: etl-secrets
  namespace: "${NS_COMPUTE}"
type: Opaque
stringData:
  AWS_ACCESS_KEY_ID: $(yaml_quote "$AWS_ACCESS_KEY_ID")
  AWS_SECRET_ACCESS_KEY: $(yaml_quote "$AWS_SECRET_ACCESS_KEY")
  TSDB_PASSWORD: $(yaml_quote "$TSDB_PASSWORD")
  GATEWAY_INTERNAL_TOKEN: $(yaml_quote "$GATEWAY_INTERNAL_TOKEN")
  GATEWAY_JWT_SECRET: $(yaml_quote "$GATEWAY_JWT_SECRET")
EOF

    echo -e "${GREEN}Generated ${CONFIGMAP_FILE}${NC}"
fi

echo ""
echo "Usage examples:"
echo "  cd app-code && uv run python -m pipelines.demo_pipeline"
echo "  cd app-code && uv run python -m pipelines.market_pulse_demo --chunk main"
