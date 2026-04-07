#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${USER_ENV_FILE:-${PROJECT_ROOT}/.env.user}"
NAMESPACE="${USER_SECRET_NAMESPACE:-etl-compute}"
TARGET="${1:-all}"

COMMON_LLM_KEYS=(
  GOOGLE_API_KEY
  OPENAI_API_KEY
  LLM_PROVIDER
  AGENT_LLM_PROVIDER
  GEMINI_MODEL
  OPENAI_MODEL
)

RAY_KEYS=(
  "${COMMON_LLM_KEYS[@]}"
  FMP_API_KEY
)

BENCHMARK_KEYS=(
  "${COMMON_LLM_KEYS[@]}"
  FMP_API_KEY
)

GATEWAY_KEYS=(
  "${COMMON_LLM_KEYS[@]}"
)

OVERSEER_KEYS=(
  "${COMMON_LLM_KEYS[@]}"
)

ALLOWED_KEYS=(
  "${COMMON_LLM_KEYS[@]}"
  FMP_API_KEY
)

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

contains_key() {
  local needle="$1"
  shift
  local item
  for item in "$@"; do
    if [[ "$item" == "$needle" ]]; then
      return 0
    fi
  done
  return 1
}

secret_name_for_target() {
  case "$1" in
    ray)
      printf '%s' "${USER_SECRET_NAME_RAY:-etl-user-secret-ray}"
      ;;
    benchmarks)
      printf '%s' "${USER_SECRET_NAME_BENCHMARKS:-etl-user-secret-benchmarks}"
      ;;
    gateway)
      printf '%s' "${USER_SECRET_NAME_GATEWAY:-etl-user-secret-gateway}"
      ;;
    overseer)
      printf '%s' "${USER_SECRET_NAME_OVERSEER:-etl-user-secret-overseer}"
      ;;
    *)
      return 1
      ;;
  esac
}

keys_for_target() {
  case "$1" in
    ray)
      printf '%s\n' "${RAY_KEYS[@]}"
      ;;
    benchmarks)
      printf '%s\n' "${BENCHMARK_KEYS[@]}"
      ;;
    gateway)
      printf '%s\n' "${GATEWAY_KEYS[@]}"
      ;;
    overseer)
      printf '%s\n' "${OVERSEER_KEYS[@]}"
      ;;
    *)
      return 1
      ;;
  esac
}

if [[ ! -f "$ENV_FILE" ]]; then
  echo "[ERROR] User env file not found: $ENV_FILE"
  echo "[INFO] Copy '.env.user.example' to '.env.user' and fill in your keys first."
  exit 1
fi

declare -A SECRET_VALUES=()

while IFS= read -r raw_line || [[ -n "$raw_line" ]]; do
  line="${raw_line%$'\r'}"
  [[ -z "$(trim "$line")" ]] && continue
  [[ "$(trim "$line")" == \#* ]] && continue
  [[ "$line" != *=* ]] && continue

  key="$(trim "${line%%=*}")"
  value="$(trim "${line#*=}")"

  if [[ "$value" =~ ^\".*\"$ ]] || [[ "$value" =~ ^\'.*\'$ ]]; then
    value="${value:1:${#value}-2}"
  fi

  if [[ -z "$value" ]]; then
    continue
  fi

  if [[ "$value" =~ ^\<.*\>$ ]]; then
    continue
  fi

  if contains_key "$key" "${ALLOWED_KEYS[@]}"; then
    SECRET_VALUES["$key"]="$value"
  fi
done < "$ENV_FILE"

if [[ "${#SECRET_VALUES[@]}" -eq 0 ]]; then
  echo "[ERROR] No supported user secret values were found in $ENV_FILE"
  echo "[INFO] Supported keys: ${ALLOWED_KEYS[*]}"
  exit 1
fi

kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 || kubectl create namespace "$NAMESPACE" >/dev/null

TARGETS=()
case "$TARGET" in
  all)
    TARGETS=(ray benchmarks gateway overseer)
    ;;
  ray|benchmarks|gateway|overseer)
    TARGETS=("$TARGET")
    ;;
  *)
    echo "[ERROR] Unknown target: $TARGET"
    echo "[INFO] Usage: bash scripts/apply-user-secrets.sh [ray|benchmarks|gateway|overseer|all]"
    exit 1
    ;;
esac

for target_name in "${TARGETS[@]}"; do
  mapfile -t target_keys < <(keys_for_target "$target_name")
  secret_name="$(secret_name_for_target "$target_name")"
  create_cmd=(
    kubectl create secret generic "$secret_name"
    -n "$NAMESPACE"
    --dry-run=client
    -o yaml
  )

  included=0
  for key in "${target_keys[@]}"; do
    if [[ -n "${SECRET_VALUES[$key]:-}" ]]; then
      create_cmd+=(--from-literal="${key}=${SECRET_VALUES[$key]}")
      included=1
    fi
  done

  if [[ "$included" -eq 0 ]]; then
    echo "[WARN] No matching values found for target '${target_name}'. Skipping."
    continue
  fi

  "${create_cmd[@]}" | kubectl apply -f -

  echo "[OK] Applied secret '${secret_name}' in namespace '${NAMESPACE}'."
  echo "[INFO] Included keys for ${target_name}:"
  for key in "${target_keys[@]}"; do
    if [[ -n "${SECRET_VALUES[$key]:-}" ]]; then
      echo "  - ${key}"
    fi
  done
  echo
done

echo "[NEXT] Restart the workloads for the targets you updated."
echo "       ray        -> reapply or restart the Ray cluster pods"
echo "       benchmarks -> rollout restart the benchmark runner deployments if they are already running"
echo "       gateway    -> kubectl rollout restart deployment/etl-gateway -n ${NAMESPACE}"
echo "       overseer   -> kubectl rollout restart deployment/etl-overseer -n ${NAMESPACE}"
