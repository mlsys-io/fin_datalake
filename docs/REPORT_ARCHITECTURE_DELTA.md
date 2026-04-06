# Report Architecture Delta

This note is for revising `final_report/main.tex` against the **current repository implementation**. It treats the codebase as source of truth, treats current repo docs as secondary support, and does **not** treat Chapter 5 as canonical.

## Canonical 5-Layer Architecture

Use this as the current architecture framing for the report:

- **Data Layer**: source and sink abstractions, Delta Lake, optional downstream sinks, metastore integration
- **Compute Layer**: Prefect flows, Ray execution, Serve hosting, BaseTask execution modes, mixed task-runner patterns
- **Intelligence Layer**: Serve-backed agents, AgentHub live discovery, ContextStore shared state, durable agent catalog
- **Control Layer**: Overseer collectors, policies, cooldowns, actuators, catalog reconciliation, recovery actions
- **Interface Layer**: REST API, MCP server, shared dispatch, auth/RBAC, audit logging, system endpoints, dashboard support

Important framing correction:

- The repo should no longer be described primarily as `Compute + Gateway + Overseer`.
- That 3-part view is still a useful operator view, but the architecture should be written as a 5-layer system with concrete components mapped into those layers.

## Report Draft Claims That Are Still Correct

- The system is built around co-locating ETL and agents on shared Ray infrastructure.
- A protocol-agnostic access pattern is still a valid design goal and is reflected in the registry-plus-adapter structure.
- MCP is a real implemented part of the access surface.
- The durable catalog plus live runtime view is central to recovery and visibility.
- The control plane is out-of-band from the managed workloads.
- Zero-copy and reduced serialization overhead remain valid parts of the system story.
- The five-layer decomposition itself is still the right architectural framing.

## Claims That Need Rewording

### Architecture framing

- Reword any repo-alignment statements that imply the repository itself is currently documented around three layers.
- Say that the current codebase is best understood through the 5-layer architecture, while some operator docs historically used a narrower `compute/gateway/overseer` view.

### Interface layer

- Reword "REST, MCP, and CLI are three peer implemented interfaces."
- Current implementation supports **REST and MCP** in the gateway.
- The repo also has operator CLIs such as `etl-agents`, but not a generic gateway CLI translator equivalent to REST and MCP.

- Reword `BrokerAdapter` as a direct-access adapter that currently returns configured credentials or connection strings.
- Do **not** describe it as already vending short-lived scoped credentials unless that implementation is added later.

- Update adapter/domain descriptions to include the `system` domain surface.

### Compute layer

- Reword any statement that implies Prefect + Ray is the universal execution path for all flows.
- Current repo uses a mix of:
  - Prefect with `RayTaskRunner`
  - Prefect with local or thread-based runners such as `ConcurrentTaskRunner`

- Reword the hybrid execution model so it does not imply `.local()` is the only current Delta workaround.
- Current implementation supports both:
  - explicit local execution
  - distributed Delta writes via Ray Data

### Intelligence layer

- Reword claims that the system has fully replaced centralized live coordination with a decentralized service mesh.
- Current implementation is more accurately:
  - **centralized live discovery through AgentHub**
  - **direct invocation through Ray Serve handles**

- Keep the service-mesh language only if you qualify it carefully and do not imply AgentHub is gone from the live path.

### Control layer

- Reword any statement that says Overseer communicates only through the Ray HTTP dashboard API.
- Current implementation also uses Ray runtime/client helpers and writes reconciliation state back into the durable catalog.

- Update the loop interval from **15 seconds** to the current configured value of **5 seconds** unless you are intentionally documenting a different deployment profile.

- Reword recovery claims so they match the demonstrated implementation:
  - deployment-level reconciliation
  - same-name respawn of managed Serve deployments
  - alerting and gateway circuit-breaker actions

## Claims That Should Be Removed or Softened

- Remove or soften any claim that a generic gateway CLI is already implemented as a first-class peer interface.
- Remove or soften any claim that broker credential vending is already short-lived or scoped.
- Remove or soften any claim that AgentHub has been removed from the critical live discovery path.
- Remove or soften any claim that Overseer has verified cluster-wide restart/orchestration behavior through KubeRay controller actions.
- Remove or soften any wording that presents gateway or overseer as still "to be implemented."

## Current Implementation Details To Mention Explicitly

- Gateway registry domains currently include:
  - `data`
  - `compute`
  - `agent`
  - `broker`
  - `system`

- Gateway currently exposes:
  - REST routes for auth, intent dispatch, agent operations, system views, and SSE alerts
  - MCP tools for data, compute, agent, broker, and system operations

- `market_pulse_ingest.py` currently uses `ConcurrentTaskRunner`, which is an important correction to any universal Ray orchestration wording.

- `BaseTask.local()` and `DeltaLakeWriteTask.local()` still exist, but current Delta write support also includes Ray Data distributed writes.

- AgentHub remains the live runtime registry for:
  - capability-based discovery
  - liveness-oriented metadata
  - selection inputs for delegation

- Delegation in the current code path is:
  - query AgentHub
  - choose a target
  - invoke the selected Ray Serve app directly

- Overseer currently:
  - collects runtime metrics
  - normalizes Serve state in the Ray collector
  - reconciles desired and observed deployment state
  - updates the durable catalog
  - issues respawn, alert, and gateway circuit-break actions

## Suggested Replacement Language

### Compute

The Compute Layer combines Prefect orchestration with Ray-backed execution patterns, but not every flow uses the same runner configuration. Some pipelines use `RayTaskRunner`, while others use Prefect-native concurrency such as `ConcurrentTaskRunner`, reflecting a pragmatic mixed execution model rather than a single universal path. The platform also retains explicit local execution helpers for runtime-sensitive I/O while supporting newer distributed Delta write paths via Ray Data.

### Intelligence

The Intelligence Layer hosts long-lived Ray Serve agents with stable deployment names. Live discovery remains centralized in AgentHub, which stores capability and liveness metadata for running agents, while actual invocation occurs directly against Ray Serve handles. This makes the current architecture best described as centralized discovery plus direct Serve invocation rather than a fully decentralized mesh with no central live registry.

### Interface

The Interface Layer is currently implemented through REST and MCP. Both surfaces route into the same shared dispatch and adapter pipeline, preserving protocol-agnostic execution semantics. Operator CLIs remain important for lifecycle management, but they should be described separately from the gateway's implemented peer interfaces.

### Control

The Control Layer runs as an out-of-band Overseer service that monitors runtime health, reconciles desired deployment state against normalized live Serve state, and records that reconciliation back into the durable catalog. In the current implementation, its demonstrated recovery scope is deployment-level healing and same-name respawn of managed Serve applications, alongside alerting and gateway circuit-breaker control.

## Practical Editing Guidance

- Prefer "current implementation" over "designed system" when the repo already contains working code.
- Where the draft describes stronger capabilities than the code currently demonstrates, either:
  - downgrade to design intent, or
  - move the statement into future work.
- If you need one safe rule while revising: when repo code and report prose disagree, rewrite the prose to match the repo unless you are explicitly labeling something as planned future work.
