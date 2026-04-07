#!/usr/bin/env bash
# =============================================================================
# Overseer Image Builder
# =============================================================================
set -euo pipefail

# Find project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

IMAGE_NAME="etl-overseer:latest"
DOCKER_CONTEXT="${PROJECT_ROOT}"
DOCKERFILE="${SCRIPT_DIR}/Dockerfile"

echo "🏗️  Building AI Lakehouse Overseer image..."
echo "📦 Context: ${DOCKER_CONTEXT}"
echo "🐳 Dockerfile: ${DOCKERFILE}"

# Build the image using the project root as context
# This allows COPY app-code to work correctly.
sudo docker build --no-cache \
    -t "${IMAGE_NAME}" \
    -f "${DOCKERFILE}" \
    "${DOCKER_CONTEXT}"

echo "✅ Successfully built ${IMAGE_NAME}"

# 💡 Reminder: 
# After building, import the image into containerd for k0s to see it:
# sudo docker save etl-overseer:latest | sudo k0s ctr images import -
