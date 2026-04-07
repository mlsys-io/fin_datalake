# Benchmark Appendix Notes

This document is a structured appendix template for the benchmark runs used in
the report. It is intentionally more detailed than the main evaluation
sections.

Use it to record:

- exact run conditions
- benchmark commands
- fairness assumptions
- observed warnings and caveats
- artifact locations
- extra result tables that are too detailed for the main body

## How To Use

For each benchmark:

1. fill in the run metadata
2. paste the exact command used
3. summarize the observed results
4. record any warnings or anomalies
5. point to the saved CSV / JSON / chart artifacts

Keep the main report concise. Use this appendix to preserve technical detail
and reproducibility.

---

## Benchmark 1: Zero-Copy vs Serialized Handoff

### Purpose

Measure end-to-end handoff latency from benchmark driver to agent-accessible
data for:

- JSON-serialized transfer
- Ray object-store zero-copy transfer

### Run Metadata

- Run date: `2026-04-06`
- Run time: `23:00:45`
- Host / pod: local benchmark driver against remote Ray cluster
- Namespace / cluster: `etl-compute` / k0s-based project cluster
- Execution mode:
  - Ray Client
- Python version:
  - cluster: `3.12.9`
  - client: `3.12.x`
- Ray version: `2.54.0`
- Serve version: `2.54.0`
- Relevant environment variables:
  - `RAY_SERVE_QUEUE_LENGTH_RESPONSE_DEADLINE_S`: default
  - others: none recorded

### Workload Definition

- Dataset sizes:
  - `10,000`
  - `100,000`
  - `500,000`
  - `1,000,000`
- Trial count per size: `30`
- Payload schema:
  - `timestamp`
  - `open`
  - `high`
  - `low`
  - `close`
  - `volume`
- Agent used: benchmark dummy table agent used by `zero_copy_benchmark.py`
- Notes on what is intentionally excluded:
  - no full Market Pulse workflow
  - no persistence timing
  - no gateway timing

### Command Used

```bash
uv run python -m pipelines.benchmarks.zero_copy_benchmark --trials 30 --json
```

### Summary Results

| Rows | JSON mean +/- std (ms) | Zero-copy mean +/- std (ms) | Speedup |
|---|---:|---:|---:|
| 10,000 | 524.02 +/- 215.33 | 60.65 +/- 11.88 | 8.64x |
| 100,000 | 3660.88 +/- 115.09 | 264.12 +/- 15.44 | 13.86x |
| 500,000 | 17594.12 +/- 283.98 | 1176.17 +/- 32.08 | 14.96x |
| 1,000,000 | 35135.61 +/- 1327.65 | 2338.79 +/- 41.32 | 15.02x |

### Observations

- Headline result: zero-copy was consistently faster, with speedup increasing with payload size and peaking at `15.02x`
- Stability across sizes: larger payload sizes were stable and support the main claim more strongly than the smallest-size run
- Any outliers: the `10,000`-row serialized path showed visibly higher jitter than the larger-size runs
- Any memory observations:
  - serialized peak memory at `10,000` rows: `5.42 MB`
  - zero-copy peak memory at `10,000` rows: `1.38 MB`

### Warnings / Caveats

- Python patch mismatch: observed between Ray cluster and client patch versions
- Ray Client / Serve warnings: Ray Client control-plane warnings appeared during the run and at shutdown
- Whether warnings occurred during execution or shutdown only: both, but the most obvious final warning was at shutdown after outputs were already written
- Whether the warnings are believed to invalidate results: no; the benchmark completed and the larger-size results were stable

### Artifact Locations

- Raw trials CSV: `app-code/benchmark-results/zero-copy/20260406-223020/zero_copy_trials.csv`
- Summary JSON: `app-code/benchmark-results/zero-copy/20260406-223020/zero_copy_summary.json`
- Chart SVG: `app-code/benchmark-results/zero-copy/20260406-223020/zero_copy_latency.svg`
- Optional text summary: `app-code/benchmark-results/zero-copy/20260406-223020/zero_copy_result.txt`

### Notes For Report

- Which figure to use: `zero_copy_latency.svg`
- Which table to use: the four-row summary table above
- Which sentence should appear in the main body: zero-copy handoff reduced transfer latency by `8.64x` to `15.02x`, with the advantage widening as dataset size increased

---

## Benchmark 2: End-to-End Comparative Latency

### Purpose

Compare three system shapes for the Market Pulse workload:

- plain sequential baseline
- Spark plus glue baseline
- integrated Market Pulse workflow

### Run Metadata

- Run date:
- Run time:
- Host / pod:
- Namespace / cluster:
- Pod specs for plain baseline:
- Pod specs for Spark baseline:
- Runtime location of integrated workflow:
- Python version(s):
- Ray version:
- Spark version:
- Relevant environment variables:

### Fairness Assumptions

- Same symbol:
- Same provider:
- Same storage endpoints:
- Same cluster / network environment:
- Shared input capture for baselines:
- Integrated workflow still runs natively:

### Command Used

```bash
# Paste exact command here
```

### Workload Definition

- Symbol:
- Provider:
- Trial count:
- Whether runs were:
  - fully live
  - partial fallback
  - fully fallback

### Summary Results

| System | Mean total latency (s) | Std (s) | P95 (s) | Success / Trials |
|---|---:|---:|---:|---:|
| Plain |  |  |  |  |
| Spark + Glue |  |  |  |  |
| Integrated |  |  |  |  |

### Integrated Workflow Stage Breakdown

| Stage | Mean (s) | Std (s) | P95 (s) |
|---|---:|---:|---:|
| Ingest |  |  |  |
| Agent setup |  |  |  |
| Signal |  |  |  |
| Persistence |  |  |  |
| Visibility |  |  |  |
| Total |  |  |  |

### Observations

- Fastest system:
- Plain vs integrated ratio:
- Spark vs integrated ratio:
- Where integrated workflow spends most time:
- Whether fallback affected any runs:

### Warnings / Caveats

- Shared-input fairness limitation:
- Live provider variability:
- Spark startup overhead:
- Any failed trials:

### Artifact Locations

- Raw trials CSV:
- Summary CSV:
- Summary JSON:
- System comparison SVG:
- Integrated stage SVG:

### Notes For Report

- Which chart to use in main body:
- Which tables go in appendix only:
- Short interpretation sentence:

---

## Benchmark 3: MTTR / Recovery

### Purpose

Compare recovery time for:

- Overseer automatic recovery
- manual scripted recovery

### Run Metadata

- Run date: `2026-04-07`
- Run time: latest clean run at `03:47:37`
- Host / pod: local benchmark driver against remote Ray / Overseer deployment
- Namespace / cluster: `etl-compute` / k0s-based project cluster
- Victim agent class: `SupportAgent`
- Trial count per mode: `30`
- Poll interval: `1.0s`
- Timeout: `120.0s`

### Recovery Definition

- MTTR start: immediately after successful delete of the victim Serve app
- MTTR end: first successful post-outage probe
- Same-name recovery required: yes
- Manual baseline includes:
  - scripted operator delay
  - recovery action
  - readiness verification

### Command Used

```bash
uv run python -m pipelines.benchmarks.mttr_recovery_benchmark --trials 30 --mode compare --seed 42 --inter-trial-delay 10 --json
```

### Summary Results

| Mode | Mean MTTR (s) | Std (s) | Min (s) | Max (s) | P95 (s) |
|---|---:|---:|---:|---:|---:|
| Overseer | 4.61 | 0.10 | 4.37 | 4.91 | 4.75 |
| Manual | 13.39 | 3.06 | 9.23 | 18.84 | 18.01 |

### Observations

- Mean improvement ratio: manual-over-overseer mean MTTR ratio was `2.91x`
- Stability of Overseer recovery: very stable; all `30/30` trials succeeded with tight variance
- Stability of manual recovery: also `30/30` successful, but with much wider spread due to operator-delay simulation
- Whether same-name recovery was consistently observed: yes

### Warnings / Caveats

- Any interference risk: earlier development runs were contaminated by stale managed baseline deployments and recovery-path implementation issues; the final run was executed only after those were cleaned up
- Any failed recoveries: none in the final 30-trial run
- Any cleanup issues: none in the final clean run
- Important interpretation note: the benchmark measures **time to usable service**, not full control-plane convergence; successful Overseer trials often still had final catalog snapshot fields at `recovering/degraded` when the endpoint probe had already succeeded

### Artifact Locations

- Raw trials CSV: `app-code/benchmark-results/mttr/20260407-034737/mttr_trials.csv`
- Summary JSON: `app-code/benchmark-results/mttr/20260407-034737/mttr_summary.json`
- Chart SVG: `app-code/benchmark-results/mttr/20260407-034737/mttr_comparison.svg`

### Notes For Report

- Main takeaway sentence: Overseer restored service in all 30 trials with a mean MTTR of `4.61s`, compared with `13.39s` for the manual baseline, representing a `2.91x` improvement in service-restoration time
- Whether to include min/max table in appendix only: yes

---

## Optional Benchmark 4: Delegation / Concurrency

### Purpose

Record only if a real repeatable delegation or load benchmark is eventually
executed.

### Run Metadata

- Run date:
- Run time:
- Host / pod:
- Namespace / cluster:

### Command Used

```bash
# Paste exact command here
```

### Summary Results

| Metric | Value |
|---|---:|
| Throughput |  |
| P95 latency |  |
| Error rate |  |

### Observations

- 

### Artifact Locations

- 

---

## Cross-Benchmark Notes

Use this section for experiment-wide details that apply to more than one
benchmark.

### Cluster / Runtime

- Cluster name:
- Node / pod specs:
- Shared storage endpoints:
- Shared secrets / env setup:

### Version Notes

- Python:
- Ray:
- Prefect:
- Spark:
- Other:

### General Caveats

- benchmark conclusions should rely on the final clean runs, not earlier debugging runs that exposed and helped fix implementation issues
- MTTR and zero-copy runs both emitted some runtime warnings, but the saved artifacts and final clean results are still usable

### Regeneration Notes

List the exact commands needed to regenerate the appendix evidence:

```bash
uv run python -m pipelines.benchmarks.zero_copy_benchmark --trials 30 --json

# comparative e2e

uv run python -m pipelines.benchmarks.mttr_recovery_benchmark --trials 30 --mode compare --seed 42 --inter-trial-delay 10 --json
```
