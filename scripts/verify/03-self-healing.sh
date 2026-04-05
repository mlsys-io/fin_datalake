#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"

print_section "Run self-healing demo"
cd "${APP_CODE_DIR}"
uv run python pipelines/self_healing_demo.py

print_section "Post-demo agent fleet"
ensure_login
auth_curl "$(gateway_url)/api/v1/agents"
printf '\n'

print_section "Post-demo Overseer snapshot"
auth_curl "$(gateway_url)/api/v1/system/overseer/snapshots?n=1"
printf '\n'
