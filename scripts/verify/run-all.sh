#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

bash "${SCRIPT_DIR}/01-deploy-baseline.sh"
bash "${SCRIPT_DIR}/02-control-plane-smoke.sh"
bash "${SCRIPT_DIR}/03-self-healing.sh"
