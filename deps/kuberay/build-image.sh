#!/usr/bin/env bash
# =============================================================================
# Build and push custom Ray image with ETL dependencies
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE_NAME="etl-ray"
IMAGE_TAG="2.9.3-py311"

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

docker build -t "${FULL_IMAGE}" "${SCRIPT_DIR}"

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
    echo "   2. Load into containerd: docker save ${FULL_IMAGE} | ctr -n k8s.io images import -"
fi

echo ""
echo "📋 Next steps:"
echo "   1. Update ray-cluster.yaml to use: ${FULL_IMAGE}"
echo "   2. kubectl apply -f ray-cluster.yaml"
