#!/usr/bin/env bash
# =============================================================================
# Build Gateway Docker image locally
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
IMAGE_NAME="etl-gateway"
IMAGE_TAG="latest"

echo "🔨 Building AI Lakehouse Gateway image..."
echo "   Image: ${IMAGE_NAME}:${IMAGE_TAG}"
echo "   Build context: ${PROJECT_ROOT}"
echo "   Dockerfile: ${SCRIPT_DIR}/Dockerfile"

# Build using project root as context so we can COPY app-code
docker build --no-cache -t "${IMAGE_NAME}:${IMAGE_TAG}" -f "${SCRIPT_DIR}/Dockerfile" "${PROJECT_ROOT}"

echo ""
echo "✅ Build complete: ${IMAGE_NAME}:${IMAGE_TAG}"
echo ""
echo "📋 Next steps:"
echo "   1. Import image into k0s: sudo docker save ${IMAGE_NAME}:${IMAGE_TAG} | sudo k0s ctr images import -"
echo "   2. Run deployment: ./deploy.sh"
