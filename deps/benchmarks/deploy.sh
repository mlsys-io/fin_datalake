#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="etl-compute"

log_step() { echo "[STEP] $1"; }
log_info() { echo "[INFO] $1"; }
log_warn() { echo "[WARN] $1"; }

require_prereqs() {
    kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 || kubectl create namespace "$NAMESPACE"

    if ! kubectl get secret etl-secrets -n "$NAMESPACE" >/dev/null 2>&1; then
        echo "[ERROR] Missing secret 'etl-secrets' in namespace '${NAMESPACE}'."
        exit 1
    fi

    if ! kubectl get configmap minio-ca -n "$NAMESPACE" >/dev/null 2>&1; then
        echo "[ERROR] Missing configmap 'minio-ca' in namespace '${NAMESPACE}'."
        exit 1
    fi
}

deploy_plain() {
    log_step "Deploying plain baseline runner"
    kubectl apply -f "${SCRIPT_DIR}/plain-baseline-deploy.yaml"
    kubectl wait --for=condition=available --timeout=120s deployment/benchmark-plain-runner -n "$NAMESPACE"
    kubectl get pods -n "$NAMESPACE" -l app=benchmark-plain-runner
}

deploy_spark() {
    log_step "Deploying Spark baseline runner"
    log_warn "Ensure docker.io/library/etl-spark-benchmark:latest-py312 is available to the cluster runtime first."
    kubectl apply -f "${SCRIPT_DIR}/spark-baseline-deploy.yaml"
    kubectl wait --for=condition=available --timeout=120s deployment/benchmark-spark-runner -n "$NAMESPACE"
    kubectl get pods -n "$NAMESPACE" -l app=benchmark-spark-runner
}

show_next_steps() {
    echo
    log_info "Runner pods are ready."
    log_info "Plain baseline shell:"
    echo "       kubectl exec -it -n ${NAMESPACE} deploy/benchmark-plain-runner -- /bin/bash"
    log_info "Spark baseline shell:"
    echo "       kubectl exec -it -n ${NAMESPACE} deploy/benchmark-spark-runner -- /bin/bash"
}

main() {
    require_prereqs
    case "${1:-all}" in
        plain)
            deploy_plain
            ;;
        spark)
            deploy_spark
            ;;
        all)
            deploy_plain
            deploy_spark
            ;;
        *)
            echo "Usage: $0 [plain|spark|all]"
            exit 1
            ;;
    esac
    show_next_steps
}

main "$@"
