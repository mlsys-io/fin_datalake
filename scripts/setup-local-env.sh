#!/bin/bash
# =============================================================================
# ETL Framework - Local Environment Setup
# =============================================================================

set -e

# Get script location and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
APP_DIR="${PROJECT_ROOT}/app-code"
ENV_FILE="${PROJECT_ROOT}/.env"
USER_ENV_FILE="${PROJECT_ROOT}/.env.user"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

load_env_file() {
    local file_path="$1"
    local label="$2"

    if [ -f "$file_path" ]; then
        set -a
        source "$file_path"
        set +a
        echo "  Loaded ${label}"
    fi
}

echo -e "${GREEN}================================================================${NC}"
echo -e "${GREEN}    ETL Framework - Local Environment Setup${NC}"
echo -e "${GREEN}================================================================${NC}"
echo ""

# 1. Setup Virtual Environment
echo -e "${YELLOW}[1/4] Setting up virtual environment in app-code...${NC}"
if [ ! -d "${APP_DIR}/.venv" ]; then
    python3 -m venv "${APP_DIR}/.venv"
    echo -e "  ${GREEN}Created .venv${NC}"
else
    echo -e "  ${GREEN}.venv already exists${NC}"
fi

source "${APP_DIR}/.venv/bin/activate"
echo "  Python: $(python --version)"

# 2. Install Dependencies
echo -e "${YELLOW}[2/4] Installing dependencies...${NC}"
pip install -e "${APP_DIR}" -q
pip install -r "${APP_DIR}/requirements-client.txt" -q
echo "  Installed etl-framework package and requirements"

# 3. Load Environment
echo -e "${YELLOW}[3/4] Loading configuration...${NC}"
if [ -f "${ENV_FILE}" ]; then
    load_env_file "${ENV_FILE}" ".env"
    if [ -f "${USER_ENV_FILE}" ]; then
        load_env_file "${USER_ENV_FILE}" ".env.user (overrides)"
    else
        echo "  .env.user not found (optional personal overrides)"
    fi
else
    echo -e "  ${RED}.env not found. Run scripts/setup-config.sh first.${NC}"
fi

# 4. Create Activation Helper
cat > "${PROJECT_ROOT}/activate.sh" << EOF
#!/bin/bash
set -a
source "${ENV_FILE}"
[ -f "${USER_ENV_FILE}" ] && source "${USER_ENV_FILE}"
set +a
source "${APP_DIR}/.venv/bin/activate"
echo "Environment activated"
EOF
chmod +x "${PROJECT_ROOT}/activate.sh"
echo -e "${GREEN}Created project-root activate.sh helper script${NC}"
echo "  Shared config: .env"
echo "  Personal override: .env.user (optional)"

echo -e "\n${GREEN}Setup Complete!${NC}"
echo "To activate:  source ./activate.sh"
