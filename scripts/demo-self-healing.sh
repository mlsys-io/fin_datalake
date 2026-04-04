#!/usr/bin/env bash
# =============================================================================
# Autonomic Self-Healing Demonstration
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "${PROJECT_ROOT}/app-code"
uv run python pipelines/self_healing_demo.py
