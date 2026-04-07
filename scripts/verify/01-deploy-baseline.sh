#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"

print_section "Deploy baseline fleet"
cd "${APP_CODE_DIR}"
uv run etl-agents deploy-baseline

print_section "List merged fleet state"
uv run etl-agents list
