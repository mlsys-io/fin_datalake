#!/usr/bin/env bash
# =============================================================================
# Build and push custom Ray image with ETL dependencies
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
IMAGE_NAME="etl-ray"
IMAGE_TAG="latest-py312"

# Use local registry or Docker Hub
# Change this to your registry
REGISTRY="${REGISTRY:-}"  # Leave empty for local-only build

if [ -n "$REGISTRY" ]; then
    FULL_IMAGE="${REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}"
else
    FULL_IMAGE="${IMAGE_NAME}:${IMAGE_TAG}"
fi

echo "🔨 Building Ray image with ETL dependencies..."
echo "   Image: ${FULL_IMAGE}"
echo "   Build context: ${PROJECT_ROOT}"
echo "   Dockerfile: ${SCRIPT_DIR}/Dockerfile"

# Build using project root as context so we can COPY app-code
docker build --no-cache -t "${FULL_IMAGE}" -f "${SCRIPT_DIR}/Dockerfile" "${PROJECT_ROOT}"

echo ""
echo "✅ Build complete: ${FULL_IMAGE}"
echo ""

if [ -n "$REGISTRY" ]; then
    echo "📤 Pushing to registry..."
    docker push "${FULL_IMAGE}"
    echo "✅ Pushed: ${FULL_IMAGE}"
else
    echo "ℹ️  No registry specified. Image is available locally."
    echo "   To use in Kubernetes, either:"
    echo "   1. Push to a registry: REGISTRY=myregistry.com ./build-image.sh"
    echo "   2. Load into containerd: docker save ${FULL_IMAGE} | sudo k0s ctr images import -"
fi

echo ""
echo "📋 Next steps:"
echo "   1. Import image: sudo docker save ${FULL_IMAGE} | sudo k0s ctr images import -"
echo "   2. Redeploy: kubectl delete raycluster etl-ray -n etl-compute && kubectl apply -f ${SCRIPT_DIR}/ray-cluster.yaml"
