# Report Future Work Delta

This note is for revising the **Limitations** and **Future Work** sections of `final_report/main.tex` against the current project state. It is written as a report-editing aid in the same spirit as `REPORT_ARCHITECTURE_DELTA.md`.

It assumes:

- the current codebase is the source of truth
- Chapter 6 is a draft and should be refined
- future work should demonstrate clear awareness of project limitations
- each enhancement should justify **why it was not implemented now**

## Framing For The Report

The strongest way to present future work is to distinguish between:

- **core dissertation contributions already demonstrated**
- **production-maturity extensions intentionally deferred**

That distinction matters because many good future enhancements are real and valuable, but they were not necessary to validate the dissertation's main contribution:

- a unified 5-layer architecture
- co-location of ETL and agent workloads
- governed access through the gateway
- autonomic deployment-level recovery through the control plane

In other words, the report should show that these extensions were deferred **deliberately**, not overlooked.

## Recommended Future Enhancements

### 1. Event-Driven Or Hybrid Control Plane

**What it is**

Augment or partially replace the polling-based Overseer loop with an event-driven mechanism that reacts immediately to runtime failures, stream disconnects, or policy triggers.

**Why it matters**

- reduces mean time to recovery for time-sensitive failures
- improves responsiveness for streaming workloads
- makes the control plane more suitable for production environments where sub-second reaction matters

**Why it was not implemented now**

- the current polling design is simpler, easier to validate, and sufficient to prove autonomic recovery
- event-driven control would require a more complex consistency model for event ordering, deduplication, and action coordination
- this would shift the project from proving the architecture to optimizing one subsystem

### 2. Gateway Command/Data Plane Separation

**What it is**

Split lightweight control operations from heavyweight data or compute operations so that monitoring, readiness, and administrative commands remain responsive under load.

**Why it matters**

- prevents expensive requests from delaying operational visibility
- supports more concurrent long-running jobs
- aligns the gateway with mature command/data plane patterns used in infrastructure systems

**Why it was not implemented now**

- the current unified dispatch path is enough to validate protocol-agnostic access
- queueing, worker pools, and asynchronous job tracking would substantially increase implementation surface
- this is primarily a production-hardening enhancement rather than a core architectural proof point

### 3. Smarter AgentHub Scheduling And Telemetry-Aware Routing

**What it is**

Evolve AgentHub from simple capability discovery into a richer coordination service that considers queue depth, latency, error rate, or resource pressure when selecting targets.

**Why it matters**

- reduces tail latency under bursty or uneven workloads
- improves fairness across agent replicas
- enables more informed scaling and service-level management

**Why it was not implemented now**

- the current registry already proves live discovery, delegation, and integration with the control plane
- telemetry-aware routing would require broader observability support and additional policy design
- the additional complexity would not materially strengthen the dissertation's main architecture claim

### 4. Temporary And Scoped Broker Credentials

**What it is**

Replace environment-backed broker credentials with short-lived, scoped credentials issued through a proper token-vending mechanism.

**Why it matters**

- improves tenant isolation and security posture
- makes the direct-access broker pattern more realistic for enterprise deployment
- reduces the risk of over-privileged long-lived credentials

**Why it was not implemented now**

- the current broker implementation is sufficient to demonstrate the architectural pattern of direct-access vending
- secure token vending requires a larger identity, policy, and secret-management design
- this would move the project into IAM engineering rather than system architecture evaluation

### 5. Prometheus And Grafana Observability Integration

**What it is**

Export control-plane and runtime metrics to Prometheus and visualize them through Grafana instead of relying mainly on Redis-backed snapshots and custom gateway views.

**Why it matters**

- improves operator visibility and historical analysis
- supports alerting, dashboards, and ecosystem-standard monitoring
- makes the platform easier to operate in a real production environment

**Why it was not implemented now**

- existing snapshots and dashboard surfaces are enough to support the current demo and validation flows
- full observability integration mainly improves operational polish rather than architectural novelty
- the time budget was better spent stabilizing the integrated system itself

### 6. Advanced RAG Integration

**What it is**

Extend the current Milvus-related components into a full retrieval-augmented pipeline where documents are chunked, embedded, indexed, and retrieved as live context for agents.

**Why it matters**

- improves reasoning quality for document-heavy use cases
- broadens the applicability of the intelligence layer beyond market data and lightweight coordination
- makes the system more compelling for enterprise knowledge workflows

**Why it was not implemented now**

- this would create a second major research track alongside the unified-fabric contribution
- it requires additional evaluation design around retrieval quality and grounding
- the current project is stronger if it stays focused on infrastructure unification rather than adding another deep AI subsystem

### 7. Multi-Language Agent Support

**What it is**

Introduce a language interoperability layer so agents written in Rust or Go can participate in the same intelligence plane alongside Python agents.

**Why it matters**

- broadens compatibility with emerging non-Python agent ecosystems
- makes the platform more realistic for heterogeneous engineering teams
- could improve performance for selected workloads

**Why it was not implemented now**

- the current Python-only model is enough to validate the architectural design
- cross-language deployment, packaging, and invocation semantics would add major engineering complexity
- language interoperability is better positioned as a future platform extension after the core architecture is stable

### 8. Federated Or Multi-Cluster Deployment

**What it is**

Support multiple coordinated clusters, potentially across sites or regions, instead of relying on a single-cluster topology.

**Why it matters**

- addresses scaling limits and head-node dependence
- supports data-sovereignty and locality requirements
- makes the system more robust in geographically distributed environments

**Why it was not implemented now**

- the dissertation already has enough scope with one end-to-end cluster
- multi-cluster coordination would multiply deployment, testing, and failure-handling complexity
- it would dilute the focus from proving the single-cluster unified architecture

### 9. Formal Verification Of Control Policies

**What it is**

Model and verify the control-plane policy logic to detect unsafe oscillation, action conflicts, or starvation conditions.

**Why it matters**

- increases trust in autonomic behavior
- is especially relevant for safety-sensitive and financial workloads
- provides stronger assurance than empirical testing alone

**Why it was not implemented now**

- the current control plane is still evolving and would benefit from stabilization before formalization
- formal methods have high upfront cost and a different skill profile from the main implementation effort
- empirical validation was the more appropriate first step for this dissertation stage

### 10. Stronger Benchmark Harness And Statistical Evaluation

**What it is**

Turn the current benchmark setup into a more repeatable experimental harness with more trials, stricter workload control, and stronger statistical reporting.

**Why it matters**

- increases confidence in performance claims
- supports more rigorous comparison against alternative architectures
- improves reproducibility for future work or publication

**Why it was not implemented now**

- the project already needs to balance system building, demo preparation, and evaluation
- benchmark hardening improves rigor, but it is not required to establish the architectural contribution
- a narrower evaluation can still be valid if the report is explicit about its scope

## Limitation To Future-Work Mapping

This mapping is useful if the report wants to transition smoothly from limitations into future work.

- **Head-node dependence**
  - Future work: federated or multi-cluster deployment, stronger control-plane resilience

- **Polling-based recovery**
  - Future work: event-driven or hybrid control loop

- **Python-only agent ecosystem**
  - Future work: multi-language agent interoperability

- **Simple capability-based discovery**
  - Future work: telemetry-aware routing, load balancing, and richer AgentHub scheduling

- **Static or environment-backed broker credentials**
  - Future work: short-lived, scoped token vending

- **Basic observability**
  - Future work: Prometheus/Grafana integration and richer per-agent telemetry

- **Limited retrieval capabilities**
  - Future work: full RAG integration with Milvus-backed retrieval

- **Benchmark scope**
  - Future work: stronger experimental harness and broader statistical validation

## Suggested Justification Style

The strongest report style is:

1. state the limitation clearly
2. explain why the enhancement would matter
3. justify why it was not implemented in the present work
4. connect the decision back to dissertation scope and contribution focus

Good pattern:

> This enhancement would improve production readiness, but it was intentionally deferred because the present work prioritised validating the unified architecture and end-to-end integration over deep optimisation of any single subsystem.

## Suggested Replacement Language

### Limitations Transition

The limitations of the present system largely reflect a deliberate trade-off in scope. This dissertation prioritised demonstrating a coherent end-to-end architecture spanning data engineering, agent execution, governed access, and autonomic recovery. As a result, several production-oriented extensions were intentionally deferred in order to keep the implementation focused on validating the core architectural claims.

### Future Work Opening

Future work should therefore focus less on broadening the conceptual scope of the system and more on deepening the maturity of its existing layers. The most important next steps are those that improve operational robustness, security, observability, and scalability while preserving the architectural principles already validated in this work.

### Future Work Closing

Taken together, these directions show that the current prototype is not a dead-end proof of concept but a credible architectural foundation. The present work establishes the integrated structure of the platform; future work would refine that structure into a more production-ready and empirically rigorous system.

## Important Corrections To Keep Consistent

When writing the future-work section, keep these current-implementation corrections aligned with the rest of the report:

- the Overseer loop is currently configured at **5 seconds** in the repo, not 15 seconds
- the broker currently returns configured credentials and connection strings; it should not yet be described as issuing short-lived scoped credentials
- AgentHub should be described as a live discovery registry that still plays a central role in capability lookup

## Practical Editing Guidance

- Treat these items as **maturity extensions**, not admissions that the architecture failed
- Avoid sounding apologetic; frame the omissions as scope control
- Prefer enhancements that naturally follow from the current implementation, not completely new unrelated ideas
- If space is limited, prioritize:
  - event-driven control plane
  - gateway command/data separation
  - smarter AgentHub scheduling
  - temporary broker credentials
  - observability integration
