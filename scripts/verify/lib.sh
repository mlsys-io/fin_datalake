#!/usr/bin/env bash

set -euo pipefail

VERIFY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${VERIFY_DIR}/../.." && pwd)"
APP_CODE_DIR="${PROJECT_ROOT}/app-code"

gateway_url() {
  printf '%s' "${GATEWAY_BASE_URL:-http://localhost:30801}"
}

frontend_url() {
  printf '%s' "${FRONTEND_BASE_URL:-http://localhost:30800}"
}

cookie_file() {
  printf '%s' "${VERIFY_COOKIE_FILE:-${PROJECT_ROOT}/cookies.txt}"
}

ensure_login() {
  if [[ -n "${GATEWAY_TOKEN:-}" ]]; then
    return 0
  fi

  if [[ -z "${GATEWAY_USER:-}" || -z "${GATEWAY_PASSWORD:-}" ]]; then
    echo "Gateway credentials not set. Export GATEWAY_USER and GATEWAY_PASSWORD, or provide GATEWAY_TOKEN."
    return 1
  fi

  curl -sS -c "$(cookie_file)" \
    -X POST "$(gateway_url)/api/v1/auth/login" \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"${GATEWAY_USER}\",\"password\":\"${GATEWAY_PASSWORD}\"}" >/dev/null
}

auth_curl() {
  if [[ -n "${GATEWAY_TOKEN:-}" ]]; then
    curl -sS -H "Authorization: Bearer ${GATEWAY_TOKEN}" "$@"
  else
    curl -sS -b "$(cookie_file)" "$@"
  fi
}

print_section() {
  printf '\n== %s ==\n' "$1"
}
