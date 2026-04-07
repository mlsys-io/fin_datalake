# Benchmark Baseline Runtimes

This directory contains Kubernetes deployment helpers for the benchmark baseline
runtimes.

The intent is to provision two comparable runner pods in the cluster before the
benchmark logic is finalized:

- `benchmark-plain-runner`
  - plain Python baseline runtime
  - uses the existing `etl-ray:latest-py312` image
- `benchmark-spark-runner`
  - Spark plus glue-code baseline runtime
  - uses a Spark-capable image built from `Dockerfile.spark-baseline`

Both runners:

- live in `etl-compute`
- use the same `etl-secrets` secret via `envFrom`
- mount the same MinIO CA bundle
- default to similar CPU and memory settings
- stay idle with `sleep infinity` so benchmark commands can be executed later

Typical workflow:

1. Build the Spark benchmark image.
2. Deploy one or both runners.
3. `kubectl exec` into the relevant pod or run commands via `kubectl exec -- ...`
4. Delete the runner deployments after benchmarking.

Examples:

- `bash deps/benchmarks/build-spark-image.sh`
- `bash deps/benchmarks/deploy.sh all`
- `kubectl get pods -n etl-compute -l benchmark-role=baseline-runner`
- `bash deps/benchmarks/delete.sh all`
