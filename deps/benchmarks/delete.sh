#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="etl-compute"

delete_plain() {
    kubectl delete deployment benchmark-plain-runner -n "$NAMESPACE" --ignore-not-found
}

delete_spark() {
    kubectl delete deployment benchmark-spark-runner -n "$NAMESPACE" --ignore-not-found
}

case "${1:-all}" in
    plain)
        delete_plain
        ;;
    spark)
        delete_spark
        ;;
    all)
        delete_plain
        delete_spark
        ;;
    *)
        echo "Usage: $0 [plain|spark|all]"
        exit 1
        ;;
esac
