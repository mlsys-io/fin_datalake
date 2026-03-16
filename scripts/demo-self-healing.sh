#!/usr/bin/env bash
# =============================================================================
# Autonomic Self-Healing Demonstration
# =============================================================================
# This script demonstrates the Overseer detecting a failed agent and 
# automatically respawning it to restore system health.
# =============================================================================

set -euo pipefail

# Get script location and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}    ETL Framework - Autonomic Self-Healing Demo${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"

# 1. Environment Check
if [ ! -f "${PROJECT_ROOT}/activate.sh" ]; then
    echo -e "${RED}❌ Environment not set up. Run scripts/setup-local-env.sh first.${NC}"
    exit 1
fi

echo -e "${YELLOW}[1/4] Activating environment...${NC}"
source "${PROJECT_ROOT}/activate.sh"

# 2. Deploy a Test Agent
echo -e "${YELLOW}[2/4] Deploying a MarketAnalyst agent...${NC}"
python3 -c "
import ray
from etl.agents.market_analyst import MarketAnalyst
ray.init(address='auto', ignore_reinit_error=True)
MarketAnalyst.deploy(name='DemoAgent-01')
print('  ✅ Agent DemoAgent-01 deployed successfully')
"

echo -e "${GREEN}  Service registry updated. Overseer is now monitoring DemoAgent-01.${NC}"

# 3. Simulate Failure
echo -e "\n${RED}[3/4] SIMULATING FAILURE: Killing DemoAgent-01...${NC}"
python3 -c "
import ray
try:
    actor = ray.get_actor('DemoAgent-01')
    ray.kill(actor)
    print('  💥 Agent DemoAgent-01 terminated.')
except Exception as e:
    print(f'  ❌ Failed to kill agent: {e}')
"

# 4. Watch for Self-Healing
echo -e "\n${YELLOW}[4/4] WATCHING FOR SELF-HEALING...${NC}"
echo -e "Waiting for Overseer to detect failure (MAPE-K cycle is 15s)..."

MAX_RETRIES=12
COUNT=0
HEALED=false

while [ $COUNT -lt $MAX_RETRIES ]; do
    COUNT=$((COUNT+1))
    echo -ne "  Checking status (Attempt $COUNT/$MAX_RETRIES)... \r"
    
    # Check if the agent exists again
    if python3 -c "import ray; ray.init(address='auto', ignore_reinit_error=True); ray.get_actor('DemoAgent-01')" &>/dev/null; then
        echo -e "\n${GREEN}✨ SUCCESS: Overseer has respawned DemoAgent-01!${NC}"
        HEALED=true
        break
    fi
    
    # Also check the system_status.md if it exists
    if [ -f "${PROJECT_ROOT}/docs/system_status.md" ]; then
        if grep -q "RESPAWN" "${PROJECT_ROOT}/docs/system_status.md"; then
             echo -e "\n${GREEN}✨ Overseer logged a RESPAWN action in system_status.md!${NC}"
        fi
    fi

    sleep 5
done

if [ "$HEALED" = false ]; then
    echo -e "\n${RED}❌ Timeout: Overseer did not heal the agent in time.${NC}"
    echo "Make sure the Overseer is running: python -m overseer.main"
    exit 1
fi

echo -e "\n${GREEN}════════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}    DEMO COMPLETE: System is self-healing.${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════════════════${NC}"
