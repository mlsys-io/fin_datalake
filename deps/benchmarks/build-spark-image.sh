#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
IMAGE_NAME="etl-spark-benchmark"
IMAGE_TAG="latest-py312"
REGISTRY="${REGISTRY:-}"

if [ -n "$REGISTRY" ]; then
    FULL_IMAGE="${REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}"
else
    FULL_IMAGE="${IMAGE_NAME}:${IMAGE_TAG}"
fi

echo "[STEP] Building Spark baseline benchmark image"
echo "[INFO] Image: ${FULL_IMAGE}"
echo "[INFO] Build context: ${PROJECT_ROOT}"

docker build --no-cache -t "${FULL_IMAGE}" -f "${SCRIPT_DIR}/Dockerfile.spark-baseline" "${PROJECT_ROOT}"

echo
echo "[INFO] Build complete: ${FULL_IMAGE}"

if [ -n "$REGISTRY" ]; then
    echo "[STEP] Pushing image to registry"
    docker push "${FULL_IMAGE}"
    echo "[INFO] Pushed: ${FULL_IMAGE}"
else
    echo "[INFO] No registry specified. Load the image into the cluster runtime if needed."
    echo "       docker save ${FULL_IMAGE} | sudo k0s ctr images import -"
fi
