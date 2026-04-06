# Report Refined Demo And Evaluation Plan

This note updates the evaluation plan to match the **current implemented Market Pulse demo** rather than the earlier looser concept. The project should now be presented as:

- one **implemented recorded showcase demo**
- several **separate quantitative benchmarks**
- several **qualitative comparisons** for product and architecture value

This remains the right structure for the dissertation and presentation because the demo explains the system, while the benchmarks justify specific novelty claims.

## Core Evaluation Principle

The project should not force one artifact to prove everything.

Instead:

1. the recorded demo shows how the five-layer platform behaves end to end
2. the benchmark suite supports a smaller set of technical novelty claims
3. the qualitative comparison explains why the platform is valuable for constrained teams

## Current Implemented Demo

The current Market Pulse demo is now a much clearer system showcase than before.

### Current Flow

**Ingestion**

- `MarketPriceIngestService` runs as a stateful Ray service
- live price ticks are ingested from WebSocket
- the service maintains a bounded latest window plus rolling market metrics
- price data is written to:
  - Delta Lake
  - RisingWave
- FMP news is fetched through the ETL task path
- news is written to Delta Lake
- the ingest subflow returns:
  - `news`
  - `ohlc`
  - `market_state`
  - `news_meta`
  - `ohlc_meta`

**Process**

- the top-level demo flow calls `market_pulse_ingest` as a subflow
- `MarketAnalystAgent` is now the specialist news-analysis agent
- `StrategyAgent` acts as the decision layer
- the flow passes:
  - `headlines`
  - `market_state`
  - optional raw `ohlc`
  to `StrategyAgent`
- `StrategyAgent` delegates headline analysis to `MarketAnalystAgent`
- the returned sentiment output is fused with market metrics into the final signal
- the signal is written to:
  - ContextStore
  - Delta Lake signal history
  - RisingWave signal history

**Interaction**

- the user still submits the workflow through the local demo entrypoint
- the interface-layer add-on queries persisted results through the gateway
- the gateway path is **query-only** for this pass
- the current query path uses the `data` domain with a dedicated RisingWave-backed `query_stream` action

### What The Demo Now Proves More Clearly

- **Data Layer**
  - ETL abstractions are actually used
  - Delta Lake and RisingWave are both active data-plane components
- **Compute Layer**
  - Prefect subflow orchestration and Ray execution remain central
- **Intelligence Layer**
  - specialist analysis agent plus fusion/decision agent are now cleaner and easier to explain
- **Control Layer**
  - self-healing remains separately demonstrable
- **Interface Layer**
  - results can be queried through the gateway rather than by reading transient state directly

## Recorded Demo Plan

The recorded demo should be treated as a **showcase artifact**, not as a benchmark.

### Recommended Showcase Sequence

### Segment 1: Architecture Framing

Show:

- the five-layer architecture
- Market Pulse as the end-to-end example workload
- the roles of:
  - Delta Lake
  - RisingWave
  - Prefect
  - Ray
  - StrategyAgent
  - MarketAnalystAgent
  - Gateway
  - Overseer

Audience takeaway:

- “I understand how the layers map to concrete components.”

### Segment 2: Market Pulse Workflow

Show:

- one workflow submission
- ingest subflow running
- price service state and metrics
- FMP news ingest
- final signal returned from the remote workflow

Important emphasis:

- the local machine is only the operator/presentation surface
- the actual workflow execution remains remote

Audience takeaway:

- “The system executes one coherent workflow, not a manually stitched script.”

### Segment 3: Agent Coordination

Show:

- `StrategyAgent` as the decision layer
- `MarketAnalystAgent` as the specialist news-analysis layer
- the final signal fields:
  - action
  - confidence
  - sentiment label/score
  - market-state evidence

Audience takeaway:

- “The intelligence layer is coordinated and explainable.”

### Segment 4: Data Plane Persistence

Show:

- price stream persistence
- signal history persistence
- Delta Lake paths
- RisingWave tables or queryable history

Audience takeaway:

- “The system is not producing a one-off signal; it is materializing queryable state.”

### Segment 5: Interface Add-On

Show:

- query persisted signal history through the gateway
- query persisted recent price history through the gateway
- optionally show joined interpretation verbally, even if the query is presented as two separate calls

Audience takeaway:

- “The interface layer exposes stored results through the existing governed gateway.”

### Segment 6: Self-Healing

Show:

- delete a managed deployment
- show the failure state
- show Overseer recover the same deployment identity

Audience takeaway:

- “The control plane adds genuine operational value.”

## Quantitative Evaluation Plan

The benchmark suite should stay narrower than the demo and be directly tied to the strongest claims.

### Benchmark 1: Zero-Copy Vs Serialized Handoff

Claim supported:

- the unified fabric reduces handoff overhead between data and intelligence workloads

Current status:

- implemented in `app-code/pipelines/benchmarks/zero_copy_benchmark.py`

What the benchmark should ultimately show:

- serialized path latency
- zero-copy path latency
- scaling across several dataset sizes

Important note:

- this remains one of the strongest technical novelty benchmarks
- the current implementation already uses the current Serve invocation pattern and a benchmark-only dummy table agent to avoid polluting the main strategy path

### Benchmark 2: End-To-End Tick-To-Signal Latency

Claim supported:

- the integrated platform is practically responsive end to end

Current status:

- implemented as a repeated-trial runtime-realistic comparative harness in `app-code/pipelines/benchmarks/benchmark_market_pulse.py`
- the current harness compares:
  - a plain sequential baseline
  - a Spark plus glue baseline
  - the integrated Market Pulse workflow

What the benchmark should show:

- repeated comparative end-to-end runs across the three system shapes
- total latency comparison between:
  - plain sequential
  - Spark plus glue
  - integrated workflow
- integrated-workflow stage timings for:
  - ingest
  - agent setup
  - signal generation
  - persistence
  - visibility
  - total workflow duration
- per-trial live vs fallback mode labeling
- success/failure counts and grouped summaries

Important note:

- this should be framed as **practical sufficiency**, not universal superiority
- the benchmark is now designed around meaningful reference points:
  - plain sequential for low-ops simplicity
  - Spark plus glue for a fragmented but scalable conventional stack
- for fairness, the two baselines consume one shared live-ish captured input per trial, while the integrated workflow still runs natively in the same trial window

### Current Benchmark Code Status

- the canonical benchmark code now lives under `app-code/pipelines/benchmarks/`
- compatibility wrappers remain under `app-code/pipelines/` so old commands still work
- the benchmark package now includes a colocated audit note documenting which requirements are already covered and which are still missing

### Benchmark 3: MTTR / Recovery

Claim supported:

- the control plane provides useful autonomic recovery

Current status:

- self-healing demo exists
- the benchmark should now be treated as a **comparative MTTR benchmark** rather than an Overseer-only measurement

What the benchmark should show:

- Overseer MTTR distribution
- manual-recovery MTTR distribution
- minimum, mean, maximum, and P95 MTTR for both
- comparative improvement using the same deploy/delete/recover path

Important note:

- this remains essential if the report is going to make a strong quantitative recovery claim
- manual recovery is the better baseline for this project than Kubernetes-style recovery because it matches the constrained-team target user story

### Optional Benchmark 4: Delegation / Concurrency

Claim supported:

- the intelligence layer supports multi-agent interaction under load

Current status:

- not yet the strongest part of the implemented benchmark suite

Recommendation:

- keep this optional unless a proper repeatable harness is built

## Qualitative Comparison Plan

These sections do not require formal benchmarking.

### Comparison 1: Integration Friction

Question answered:

- how much platform engineering burden is removed compared with a fragmented stack?

Use:

- architecture comparison
- deployment-surface comparison
- amount of glue/orchestration that the platform hides from the end user

### Comparison 2: Interface And Access Usability

Question answered:

- can users access results without writing bespoke integration logic?

Use:

- local workflow submission
- gateway result querying
- governed access through the same interface layer

### Comparison 3: Operational Simplicity

Question answered:

- how much deployment and MLOps burden is removed?

Use:

- persistent agent deployment model
- self-healing path
- single integrated runtime story

## Claim-To-Evidence Matrix

### Claim: Unified Fabric

Best evidence:

- zero-copy benchmark
- implemented Market Pulse workflow

### Claim: Stateful Streaming Data Plane

Best evidence:

- price service metrics and bounded state
- Delta + RisingWave persistence
- gateway history query add-on

### Claim: Coordinated Intelligence Layer

Best evidence:

- `StrategyAgent` + `MarketAnalystAgent` flow
- final returned signal with supporting evidence

### Claim: Autonomic Recovery

Best evidence:

- self-healing walkthrough
- MTTR benchmark

### Claim: Protocol / Interface Value

Best evidence:

- gateway query add-on
- qualitative access-layer comparison

### Claim: Reduced Integration Burden

Best evidence:

- qualitative comparison against fragmented conventional stacks

## Practical Presentation Guidance

- treat the Market Pulse workflow as the **main explanatory demo**
- treat gateway querying as a **short interface-layer add-on**
- treat self-healing as a **separate operational proof**
- treat benchmark figures as **precomputed evidence**, not live experiments

## Strategic Summary

The strongest evaluation story is now:

- one implemented and explainable end-to-end demo
- one smaller set of focused benchmarks
- one qualitative argument about accessibility, integration burden, and practical usefulness

That is a better fit for the current project positioning than attempting to prove everything through a single benchmarkable demo artifact.
