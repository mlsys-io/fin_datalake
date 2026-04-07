# Market Pulse Final Demo Plan

This document is the single final reference for the Market Pulse demo. It combines the earlier detailed build plan and the tighter presentation plan into one concise demo blueprint.

## Demo Goal

Deliver a 15-minute live demo that proves the platform's core value:

- distributed ETL and AI agents run on the same compute fabric
- the gateway exposes the system through a unified interface
- the overseer detects failure and restores managed deployments
- the system can be benchmarked against simpler baselines

## Demo Story

> A Prefect pipeline ingests financial news and live market ticks into Delta Lake. Two Ray Serve agents collaborate to produce a trading signal. The gateway exposes the results, and the overseer self-heals a failed agent live.

Suggested closing line on screen:

`BUY BTCUSD - 87% confidence (bullish sentiment + upward price trend)`

## What The Demo Proves

| Novelty | Demo evidence |
|---|---|
| Unified fabric | ETL pipeline and agents share the same Ray-based runtime and storage layer |
| Persistent AI agents | Serve-backed agents stay addressable by stable deployment name |
| Agent coordination | Strategy logic delegates sentiment work to another agent |
| Autonomic recovery | Overseer restores a missing deployment with the same name |
| Protocol-agnostic access | Gateway exposes the system through REST and MCP |

## End-To-End Flow

1. Ingest news headlines through a REST source.
2. Ingest market ticks through a WebSocket source.
3. Write both feeds to Delta Lake.
4. Deploy or connect to the sentiment and strategy agents.
5. Run strategy analysis:
   - compute a trend score from recent prices
   - delegate headline scoring to the sentiment agent
   - combine both into a BUY, SELL, or HOLD signal
6. Publish the signal to shared state and optionally to TimescaleDB.
7. Query the result through the gateway.
8. Run the self-healing demo by deleting a managed Serve app and letting Overseer recover it.

## Core Components

### Data Sources

- REST news feed
- WebSocket price feed
- local fallback data when live external feeds are not available

### Pipelines

- `market_pulse_ingest.py`: news + OHLC ingestion into Delta Lake
- `market_pulse_demo.py`: deploy agents, run analysis, print final signal
- `self_healing_demo.py`: delete a managed Serve deployment and verify Overseer restores it
- `mcp_demo.py`: optional MCP-facing demo of gateway protocol access

### Agents

- `SentimentAgent`: scores headlines with an LLM-backed or heuristic fallback
- `StrategyAgent`: computes a trend score and delegates sentiment scoring before emitting a signal

### Platform Services

- Ray / Ray Serve: compute and agent hosting
- Gateway: external API and dashboard backend
- Overseer: recovery control plane
- Delta Lake: durable bronze data store
- ContextStore and AgentHub: live coordination utilities inside compute

## Demo Sequence

| Segment | What to show | Approx. time |
|---|---|---|
| Ingest | News and price data flowing into Delta Lake | 3 min |
| Agent analysis | Strategy agent delegates and emits a signal | 3 min |
| Gateway access | Query agents or signal via REST or MCP | 2 min |
| Self-healing | Remove one managed deployment and show Overseer restore it | 3 min |
| Benchmark summary | Show measured latency and comparison table | 3 min |
| Wrap-up | Re-state differentiators | 1 min |

## Benchmark Story

Use simple baselines that make the platform advantages easy to explain:

- Plain Python: sequential script, no orchestration, no recovery, no persistent agents
- Spark + glue baseline: fragmented but scalable conventional stack with more operational overhead
- Integrated workflow: unified ETL, runtime, agents, persistence, and recovery story

Suggested comparison dimensions:

- distributed ETL
- real-time streaming
- persistent agents
- delegation and shared state
- self-healing
- unified API surface

## Benchmark Observations

### Zero-Copy Benchmark

Observed result from `app-code/benchmark-results/zero-copy/20260406-223020/`:

| Rows | JSON mean +/- std (ms) | Zero-copy mean +/- std (ms) | Speedup |
|---|---:|---:|---:|
| 10,000 | 524.02 +/- 215.33 | 60.65 +/- 11.88 | 8.64x |
| 100,000 | 3660.88 +/- 115.09 | 264.12 +/- 15.44 | 13.86x |
| 500,000 | 17594.12 +/- 283.98 | 1176.17 +/- 32.08 | 14.96x |
| 1,000,000 | 35135.61 +/- 1327.65 | 2338.79 +/- 41.32 | 15.02x |

Key observation:

- the zero-copy path stayed consistently faster than the JSON-serialized path
- the speedup increased with payload size, peaking at about `15.02x` on `1,000,000` rows
- memory behavior also improved in the smoke run:
  - serialized peak memory at `10,000` rows: `5.42 MB`
  - zero-copy peak memory at `10,000` rows: `1.38 MB`

Interpretation to present:

- this benchmark supports the claim that the shared Ray fabric reduces handoff overhead between data processing and intelligence workloads
- the advantage becomes more pronounced as payload size grows, which is the practical regime where serialization overhead matters most
- the larger-size runs were stable, while the smallest-size runs showed more jitter because control-plane overhead is a larger fraction of the total runtime

Runtime caveat:

- the benchmark completed successfully and saved all artifacts, but the run showed Ray Client control-plane warnings during execution
- these warnings are best described as runtime noise rather than benchmark-logic failure
- if needed, mention that the larger-payload results were stable and remained the stronger evidence

Artifacts:

- `app-code/benchmark-results/zero-copy/20260406-223020/zero_copy_result.txt`
- `app-code/benchmark-results/zero-copy/20260406-223020/zero_copy_summary.json`
- `app-code/benchmark-results/zero-copy/20260406-223020/zero_copy_latency.svg`

### MTTR Benchmark

Observed result from `app-code/benchmark-results/mttr/20260407-034737/`:

| Mode | Trials | Successes | Mean MTTR (s) | Std (s) | P95 (s) |
|---|---:|---:|---:|---:|---:|
| Overseer | 30 | 30 | 4.61 | 0.10 | 4.75 |
| Manual | 30 | 30 | 13.39 | 3.06 | 18.01 |

Key observation:

- Overseer restored service successfully in all 30 trials
- the manual baseline also succeeded in all 30 trials, but was much slower and more variable
- the measured improvement was:
  - `2.91x` faster than the manual baseline
  - `65.59%` lower mean MTTR

Interpretation to present:

- this benchmark supports the claim that the control plane provides useful autonomic recovery
- Overseer reduced service-restoration time by removing operator reaction delay and reacting immediately to deployment loss
- the low variance in Overseer recovery also supports the claim that the control loop converges consistently once the implementation issues were fixed

Important nuance:

- this MTTR benchmark measures **time to usable service**, not full control-plane convergence
- in the raw Overseer trials, the endpoint probe had already succeeded while the catalog snapshot still briefly reported `recovering` and `degraded`
- this should be described as conservative internal reconciliation lag rather than failed recovery

Artifacts:

- `app-code/benchmark-results/mttr/20260407-034737/mttr_summary.json`
- `app-code/benchmark-results/mttr/20260407-034737/mttr_trials.csv`
- `app-code/benchmark-results/mttr/20260407-034737/mttr_comparison.svg`
- `app-code/benchmark-results/mttr/20260407-034737/mttr_writeup.md`

## Recommended Operator Scripts

### Core Platform

- `scripts/setup-config.sh`
- `scripts/apply-user-secrets.sh`
- `scripts/deploy-infrastructure.sh`
- `scripts/setup-local-env.sh`
- `scripts/cleanup-cluster.sh`

### Demo / Verification

- `scripts/verify/01-deploy-baseline.sh`
- `scripts/verify/02-control-plane-smoke.sh`
- `scripts/verify/03-self-healing.sh`
- `scripts/verify/run-all.sh`
- `scripts/demo-self-healing.sh`

### Overseer

- `deps/overseer/build-image.sh`
- `deps/overseer/deploy.sh`
- `deps/overseer/overseer-deploy.yaml`
- `deps/overseer/overseer-deploy-mttr.yaml`

### Benchmark Baseline Runners

- `deps/benchmarks/build-spark-image.sh`
- `deps/benchmarks/deploy.sh`
- `deps/benchmarks/delete.sh`
- `deps/benchmarks/plain-baseline-deploy.yaml`
- `deps/benchmarks/spark-baseline-deploy.yaml`

### Gateway / Ray / Data Services

- `deps/gateway/build-image.sh`
- `deps/gateway/deploy.sh`
- `deps/kuberay/build-image.sh`
- `deps/kuberay/deploy.sh`
- `deps/prefect/deploy.sh`
- `deps/risingwave/deploy.sh`
- `deps/risingwave/check_connection.sh`
- `deps/timescaledb/deploy.sh`
- `deps/redis/deploy.sh`

## Pre-Demo Checklist

- Baseline fleet is deployed and healthy.
- `/api/v1/agents` shows runtime-backed healthy agents.
- latest Overseer snapshot shows `agent_control` healthy.
- self-healing demo passes and cleanup returns the system to baseline.
- gateway auth and tunnel setup are working before the presentation starts.
- fallback data path is ready in case live external data is unavailable.

## Success Criteria

The demo is successful if you can show, in one coherent narrative:

- data ingestion
- agent collaboration
- unified gateway visibility
- overseer recovery
- a clear comparison advantage over simpler baselines
