# Benchmark Audit

This package is the canonical home for the Market Pulse benchmark code.

## Current Files

- `benchmark_market_pulse.py`
  - master comparative harness for plain, Spark plus glue, and integrated Market Pulse runs
- `zero_copy_benchmark.py`
  - technical benchmark for zero-copy vs serialized handoff
- `mttr_recovery_benchmark.py`
  - comparative recovery benchmark for Overseer vs manual recovery
- `baselines/baseline_plain.py`
  - direct sequential baseline using shared live-ish benchmark inputs and direct persistence
- `baselines/baseline_spark_glue.py`
  - Spark plus manual glue-code baseline representing a fragmented but scalable conventional stack

Compatibility wrappers remain in `pipelines/` for the top-level benchmark entrypoints. Synthetic baselines now live only under `pipelines/benchmarks/baselines/`.

## Evaluation Against Current Requirements

### Benchmark 1: Zero-Copy vs Serialized Handoff

- Status: implemented as a report-grade v1 harness
- Current file: `zero_copy_benchmark.py`
- Strength:
  - directly targets the zero-copy novelty claim
  - uses report-aligned market-table sizes: `10k`, `100k`, `500k`, `1M`
  - runs repeated trials and emits raw CSV, summary JSON, and an SVG chart
- Gaps:
  - still uses a narrow synthetic market-table payload rather than the full Market Pulse workflow contract
  - currently reports descriptive statistics only, not significance testing

### Benchmark 2: End-to-End Tick-to-Signal Latency

- Status: implemented as a runtime-realistic comparative repeated-trial harness
- Current file: `benchmark_market_pulse.py`
- Strength:
  - compares three system shapes:
    - plain sequential baseline
    - Spark plus glue baseline
    - integrated Market Pulse workflow
  - emits repeated-trial CSV, a report-friendly summary CSV, summary JSON, and SVG comparison charts
  - reports comparative total latency and integrated-workflow stage timings
  - captures per-trial success/failure and mode labeling for each system
  - uses one shared live-ish input capture per trial for the two baselines while keeping the integrated workflow native
- Gaps:
  - the integrated workflow still ingests natively rather than consuming the shared captured payload
  - current statistics are descriptive only

### Benchmark 3: MTTR / Recovery

- Status: implemented as a comparative repeated-trial harness
- Current file: `mttr_recovery_benchmark.py`
- Strength:
  - compares Overseer recovery against a manual-recovery baseline using the same managed deploy/delete/recover path
  - emits raw CSV, summary JSON, and an SVG chart
  - reports min/mean/max and P95 MTTR
- Gaps:
  - currently compares only against manual intervention, not an external orchestration platform
  - currently reports descriptive statistics only

### Benchmark 4: Delegation / Concurrency

- Status: optional and not yet solid
- Gap:
  - there is no strong repeatable harness yet for concurrent multi-agent delegation
- Recommendation:
  - treat this as optional unless a real load-oriented benchmark is added

## What Is Good Enough Today

- a narrow report-grade zero-copy benchmark
- a runtime-realistic comparative end-to-end Market Pulse benchmark
- a comparative MTTR benchmark aligned to the constrained-team user story

## What Still Needs Work Before Strong Report Claims

- optional delegation benchmark only if it becomes stable and repeatable
