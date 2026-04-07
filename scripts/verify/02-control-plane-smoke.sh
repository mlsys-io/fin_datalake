#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"

ensure_login

print_section "Gateway liveness"
curl -sS "$(gateway_url)/healthz"
printf '\n'

print_section "Gateway readiness"
auth_curl "$(gateway_url)/readyz"
printf '\n'

print_section "Agent fleet"
auth_curl "$(gateway_url)/api/v1/agents"
printf '\n'

print_section "Latest Overseer snapshot"
auth_curl "$(gateway_url)/api/v1/system/overseer/snapshots?n=1"
printf '\n'

print_section "Frontend route check"
printf 'Open %s/agents, %s/overseer, and %s/not-real in a browser.\n' "$(frontend_url)" "$(frontend_url)" "$(frontend_url)"
