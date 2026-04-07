# Market Pulse Demo Instructions

This document is the operator-facing walkthrough for the current Market Pulse demo. It focuses on:

- what the demo flow is
- what to record or showcase
- what points to mention in each section

The demo is designed as a **recordable systems showcase**, not a benchmark run.

## Demo Flow

### Main Workflow

1. Submit the Market Pulse workflow.
2. The `market_pulse_ingest` subflow runs.
3. `MarketPriceIngestService` ingests tick data, maintains a bounded latest window, computes rolling market metrics, and writes price state to:
   - Delta Lake
   - RisingWave
4. FMP news is fetched and written to Delta Lake.
5. The latest bounded market window and `market_state` are returned from the ingest subflow.
6. `StrategyAgent` receives:
   - `headlines`
   - `market_state`
   - optional raw `ohlc_data`
7. `StrategyAgent` delegates headline analysis to `MarketAnalystAgent`.
8. `StrategyAgent` fuses sentiment and market metrics into the final signal.
9. The signal is written to:
   - ContextStore
   - Delta Lake signal history
   - RisingWave signal history
10. The workflow returns a compact final result for local presentation.

### Interface Add-On

1. Query persisted signal history through the gateway.
2. Query persisted recent price history through the gateway.
3. Show that the interface layer accesses persisted results rather than transient in-memory state only.

### Recovery Add-On

1. Trigger the self-healing demo separately.
2. Delete a managed deployment.
3. Show Overseer restore the same deployment identity.

## Recommended Demo Chunks

### Chunk 1: Main Workflow

Suggested command:

```bash
uv run python -m pipelines.market_pulse_demo --chunk main
```

What to record / show:

- the workflow submission
- the main terminal logs
- the final `rich` summary

What to say:

- this is one workflow submission, not a manually stitched local script
- the local machine is acting as the submitter and presentation surface
- the actual execution remains remote in Prefect/Ray

Key points to mention:

- `market_pulse_ingest` is a reusable subflow
- the price path is stateful and service-based
- the news path is ETL task-based
- the workflow returns one final signal result

### Chunk 2: Data Plane Emphasis

What to record / show:

- price service logs if useful
- the summary fields for:
  - last price
  - SMA 5
  - SMA 20
  - VWAP
  - return
  - volatility
- the persistence targets:
  - Delta Lake
  - RisingWave

What to say:

- the Data Layer is not just storing raw files
- the service is maintaining bounded rolling state and materializing it
- Delta is the durable analytical store
- RisingWave is the stream-oriented query surface

Key points to mention:

- the service is stateful
- it computes metrics continuously
- persisted history makes later querying possible

### Chunk 3: Intelligence Layer Emphasis

What to record / show:

- the final signal output
- sentiment label and score
- analyst summary
- market-state fields used in the decision

What to say:

- `MarketAnalystAgent` is the specialist news-analysis agent
- `StrategyAgent` is the fusion/decision agent
- the intelligence layer is therefore coordinated, not monolithic

Key points to mention:

- specialist agent role
- delegation
- explainable final payload rather than an opaque result

### Chunk 4: Interface Layer Add-On

Suggested command:

```bash
uv run python -m pipelines.market_pulse_demo --chunk query
```

What to record / show:

- gateway-backed query output for recent signals
- gateway-backed query output for recent prices

What to say:

- this is aligned to the existing gateway implementation
- the add-on uses the `data` domain through `/api/v1/intent`
- the interface layer is querying persisted results, not peeking into internal state directly

Key points to mention:

- interface is query-only in this pass
- governed access is demonstrated without overloading the main workflow

### Chunk 5: Control Layer Add-On

Suggested command:

```bash
uv run python -m pipelines.market_pulse_demo --chunk recovery
```

What to record / show:

- deployment deletion
- missing/degraded state
- restored same-name deployment

What to say:

- self-healing is intentionally demonstrated as a separate operational path
- this keeps the normal workflow demo clean while still proving the Control Layer

Key points to mention:

- Overseer is deployment-aware
- recovery is same-identity recovery
- this is a platform behavior, not manual repair

## Suggested Recording Structure

### Segment 1: Architecture Framing

Show:

- one architecture slide or diagram

Bring up:

- five-layer architecture
- Market Pulse as the integrated example

### Segment 2: Main Workflow

Show:

- `--chunk main`

Bring up:

- subflow orchestration
- stateful price service
- remote agents
- final signal

### Segment 3: Interface Add-On

Show:

- `--chunk query`

Bring up:

- gateway alignment
- persisted result access
- interface-layer completeness

### Segment 4: Recovery Add-On

Show:

- `--chunk recovery`

Bring up:

- control-plane value
- same-name recovery

## What To Emphasize In The Presentation

- the project is not claiming to be a production trading system
- the signal is a demonstrative workload for validating the platform architecture
- the main contribution is architectural integration:
  - ETL
  - streaming state
  - agent coordination
  - governed interface
  - recovery
- the platform is aimed at teams without the capacity to build a fragmented in-house stack

## What Not To Overclaim

- do not present the signal as evidence of financial alpha
- do not imply the gateway submission path is already the main interaction mode if you are only showing query access
- do not overstate RisingWave as a separately benchmarked core contribution unless that benchmark is added later
- do not treat the showcase demo as a formal benchmark

## Benchmark Talking Points

Use benchmark numbers as precomputed supporting evidence rather than live demo content.

### Zero-Copy

- latest artifact folder:
  - `app-code/benchmark-results/zero-copy/20260406-223020/`
- headline number:
  - zero-copy reduced handoff latency by `8.64x` to `15.02x` depending on payload size
- safe interpretation:
  - the shared fabric materially reduces transport overhead between data and intelligence workloads, especially at larger payload sizes

### MTTR

- latest artifact folder:
  - `app-code/benchmark-results/mttr/20260407-034737/`
- headline number:
  - Overseer mean MTTR `4.61s` vs manual mean MTTR `13.39s`
  - `2.91x` faster service restoration
- safe interpretation:
  - this is a **service-restoration** benchmark
  - internal recovery-state fields may still briefly lag behind successful endpoint recovery

## Practical Notes

- if live external data is flaky, the fallback path is acceptable as long as it is explained honestly
- benchmark runs should not be recorded as live experiments unless they are already stable and fast
- the most important thing is that each chunk is clear, coherent, and easy to narrate

## Operator Script Reference

Use these scripts as the main operational entrypoints during setup, verification, and benchmarking.

### Platform Setup

- `scripts/setup-config.sh`
  Creates or refreshes config and secret-facing setup.
- `scripts/apply-user-secrets.sh`
  Reads `.env.user` and applies scoped optional secrets such as `etl-user-secret-ray`, `etl-user-secret-benchmarks`, `etl-user-secret-gateway`, and `etl-user-secret-overseer`.
- `scripts/deploy-infrastructure.sh`
  Main infrastructure bring-up helper.
- `scripts/setup-local-env.sh`
  Local environment setup.
- `scripts/cleanup-cluster.sh`
  Cleanup/reset helper.

### Demo Verification

- `scripts/verify/01-deploy-baseline.sh`
  Deploys the demo baseline agent fleet.
- `scripts/verify/02-control-plane-smoke.sh`
  Control-plane smoke verification.
- `scripts/verify/03-self-healing.sh`
  Self-healing verification flow.
- `scripts/verify/run-all.sh`
  Runs the main verification chain.
- `scripts/demo-self-healing.sh`
  Small self-healing demo helper.

### Overseer

- `deps/overseer/build-image.sh`
  Builds the Overseer image.
- `deps/overseer/deploy.sh`
  Deploys Overseer in normal mode or MTTR mode with `--mttr`.
- `deps/overseer/overseer-deploy.yaml`
  Normal Overseer deployment manifest.
- `deps/overseer/overseer-deploy-mttr.yaml`
  Recovery-only benchmark deployment manifest.

### Benchmark Baselines

- `deps/benchmarks/build-spark-image.sh`
  Builds the Spark baseline image.
- `deps/benchmarks/deploy.sh`
  Deploys `benchmark-plain-runner` and/or `benchmark-spark-runner`.
- `deps/benchmarks/delete.sh`
  Removes benchmark baseline runner deployments.
- `deps/benchmarks/plain-baseline-deploy.yaml`
  Plain baseline runner manifest.
- `deps/benchmarks/spark-baseline-deploy.yaml`
  Spark baseline runner manifest.

### Other Deployment Helpers

- `deps/gateway/build-image.sh`
- `deps/gateway/deploy.sh`
- `deps/gateway/down.sh`
- `deps/kuberay/build-image.sh`
- `deps/kuberay/deploy.sh`
- `deps/prefect/deploy.sh`
- `deps/risingwave/deploy.sh`
- `deps/risingwave/check_connection.sh`
- `deps/timescaledb/deploy.sh`
- `deps/redis/deploy.sh`
