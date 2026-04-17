#!/bin/bash
# =============================================================================
# ETL Framework - Local Environment Setup
# =============================================================================
#
# Modernized for uv:
# - syncs dependencies into app-code/.venv via uv
# - keeps local env variables in a helper script
# - avoids manual venv / pip management
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
APP_DIR="${PROJECT_ROOT}/app-code"
ENV_FILE="${PROJECT_ROOT}/.env"
USER_ENV_FILE="${PROJECT_ROOT}/.env.user"
HELPER_FILE="${PROJECT_ROOT}/activate.sh"
ENV_HELPER_FILE="${PROJECT_ROOT}/env.sh"

UV_EXTRAS="${UV_EXTRAS:-client}"
UV_GROUPS="${UV_GROUPS:-dev}"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

usage() {
    cat <<EOF
Usage: ./setup-local-env.sh

Optional overrides:
  UV_EXTRAS=client,gateway,overseer
  UV_GROUPS=dev
EOF
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    usage
    exit 0
fi

if ! command -v uv >/dev/null 2>&1; then
    echo -e "${RED}uv not found.${NC}"
    echo "Install uv first: https://docs.astral.sh/uv/"
    exit 1
fi

echo -e "${GREEN}================================================================${NC}"
echo -e "${GREEN}    ETL Framework - Local Environment Setup${NC}"
echo -e "${GREEN}================================================================${NC}"
echo ""
echo "Using uv: $(uv --version)"

IFS=',' read -r -a EXTRA_LIST <<< "$UV_EXTRAS"
IFS=',' read -r -a GROUP_LIST <<< "$UV_GROUPS"

SYNC_ARGS=(sync --project "$APP_DIR")

for extra in "${EXTRA_LIST[@]}"; do
    extra="${extra// /}"
    if [[ -n "$extra" ]]; then
        SYNC_ARGS+=(--extra "$extra")
    fi
done

for group in "${GROUP_LIST[@]}"; do
    group="${group// /}"
    if [[ -n "$group" ]]; then
        SYNC_ARGS+=(--group "$group")
    fi
done

echo -e "${YELLOW}[1/3] Syncing dependencies with uv...${NC}"
uv "${SYNC_ARGS[@]}"
echo "  Synchronized app-code/.venv"

echo -e "${YELLOW}[2/3] Checking local environment files...${NC}"
if [[ -f "$ENV_FILE" ]]; then
    echo "  Found .env"
else
    echo -e "  ${YELLOW}.env not found. Run scripts/setup-config.sh first.${NC}"
fi

if [[ -f "$USER_ENV_FILE" ]]; then
    echo "  Found .env.user"
else
    echo "  .env.user not found (optional personal overrides)"
fi

echo -e "${YELLOW}[3/3] Writing helper scripts...${NC}"
cat > "$ENV_HELPER_FILE" <<EOF
#!/bin/bash
set -a
[ -f "${ENV_FILE}" ] && source "${ENV_FILE}"
[ -f "${USER_ENV_FILE}" ] && source "${USER_ENV_FILE}"
set +a
echo "Environment variables loaded. Use uv run from app-code for commands."
EOF
chmod +x "$ENV_HELPER_FILE"

cat > "$HELPER_FILE" <<EOF
#!/bin/bash
source "${ENV_HELPER_FILE}"
EOF
chmod +x "$HELPER_FILE"

echo -e "${GREEN}Created project-root helper scripts:${NC}"
echo "  source ./env.sh"
echo "  source ./activate.sh   # compatibility wrapper"
echo ""
echo "Run commands with uv from app-code, for example:"
echo "  cd app-code && uv run etl-agents agents list"
echo "  cd app-code && uv run python -m pipelines.market_pulse_demo --chunk main"
echo ""
echo -e "${GREEN}Setup complete.${NC}"
