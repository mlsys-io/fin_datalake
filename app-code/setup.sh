#!/bin/bash
# =============================================================================
# ETL Framework Setup Script
# Sets up the local development environment for running pipelines
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}════════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}    ETL Framework - Local Environment Setup${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════════════════${NC}"
echo ""

# -----------------------------------------------------------------------------
# 1. Check/Create Virtual Environment
# -----------------------------------------------------------------------------
echo -e "${YELLOW}[1/5] Setting up virtual environment...${NC}"

if [ ! -d "${SCRIPT_DIR}/.venv" ]; then
    echo "  Creating virtual environment..."
    python3 -m venv "${SCRIPT_DIR}/.venv"
    echo -e "  ${GREEN}✅ Created .venv${NC}"
else
    echo -e "  ${GREEN}✅ .venv already exists${NC}"
fi

source "${SCRIPT_DIR}/.venv/bin/activate"
echo "  Python: $(python --version)"

# -----------------------------------------------------------------------------
# 2. Install Dependencies
# -----------------------------------------------------------------------------
echo -e "${YELLOW}[2/5] Installing dependencies...${NC}"

# Install package in editable mode
pip install -e "${SCRIPT_DIR}" -q
echo "  ✅ Installed etl-framework package"

# Install client requirements
pip install -r "${SCRIPT_DIR}/requirements-client.txt" -q
echo "  ✅ Installed client requirements"

# -----------------------------------------------------------------------------
# 3. Load Environment Variables
# -----------------------------------------------------------------------------
echo -e "${YELLOW}[3/5] Loading environment variables...${NC}"

if [ -f "${PROJECT_ROOT}/.env" ]; then
    source "${PROJECT_ROOT}/.env"
    echo "  ✅ Loaded .env"
    echo "  NODE_IP: ${NODE_IP:-not set}"
    echo "  RAY_ADDRESS: ${RAY_ADDRESS:-not set}"
    echo "  PREFECT_API_URL: ${PREFECT_API_URL:-not set}"
else
    echo -e "  ${RED}❌ .env not found. Run setup-config.sh first:${NC}"
    echo "     cd ${PROJECT_ROOT} && bash setup-config.sh"
    exit 1
fi

# -----------------------------------------------------------------------------
# 4. Configure Prefect
# -----------------------------------------------------------------------------
echo -e "${YELLOW}[4/5] Configuring Prefect...${NC}"

if [ -n "$PREFECT_API_URL" ]; then
    prefect config set PREFECT_API_URL="$PREFECT_API_URL" 2>/dev/null || true
    echo "  ✅ Set PREFECT_API_URL=$PREFECT_API_URL"
else
    echo -e "  ${RED}❌ PREFECT_API_URL not set in .env${NC}"
fi

# -----------------------------------------------------------------------------
# 5. Verify Ray Connection (optional)
# -----------------------------------------------------------------------------
echo -e "${YELLOW}[5/5] Verifying Ray connection...${NC}"

if [ -n "$RAY_ADDRESS" ]; then
    python3 -c "
import ray
try:
    ray.init(address='$RAY_ADDRESS', ignore_reinit_error=True)
    resources = ray.cluster_resources()
    print(f'  ✅ Connected to Ray cluster')
    print(f'     CPUs: {resources.get(\"CPU\", 0)}')
    print(f'     Memory: {resources.get(\"memory\", 0) / 1e9:.1f} GB')
    ray.shutdown()
except Exception as e:
    print(f'  ⚠️  Could not connect to Ray: {e}')
    print(f'     This may be OK if the cluster is still starting.')
"
else
    echo -e "  ${RED}❌ RAY_ADDRESS not set${NC}"
fi

# -----------------------------------------------------------------------------
# Done
# -----------------------------------------------------------------------------
echo ""
echo -e "${GREEN}════════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}    Setup Complete!${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════════════════${NC}"
echo ""
echo "To activate the environment in a new terminal:"
echo ""
echo "  source ${SCRIPT_DIR}/activate.sh"
echo ""
echo "Or manually:"
echo ""
echo "  source ${PROJECT_ROOT}/.env"
echo "  source ${SCRIPT_DIR}/.venv/bin/activate"
echo ""

# Create activate helper script
cat > "${SCRIPT_DIR}/activate.sh" << 'EOF'
#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
source "${PROJECT_ROOT}/.env"
source "${SCRIPT_DIR}/.venv/bin/activate"
echo "✅ Environment activated"
echo "   RAY: $RAY_ADDRESS"
echo "   PREFECT: $PREFECT_API_URL"
EOF
chmod +x "${SCRIPT_DIR}/activate.sh"
echo "Created ${SCRIPT_DIR}/activate.sh helper script"
