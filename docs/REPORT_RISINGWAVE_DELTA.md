# Report RisingWave Delta

This note is for revising the report around the role of **RisingWave** in the project. It is intended to correct both under-description and over-claiming.

It reflects the current repo state:

- RisingWave is still present in infrastructure and configuration
- a `RisingWaveSource` exists in the ETL layer
- RisingWave was part of the intended Data Layer architecture
- but the final validated workflows, demo path, and report narrative do not currently operationalize it as a first-class end-to-end component

## Core Position

The most accurate way to describe RisingWave is:

- **architecturally provisioned**
- **partially implementation-ready**
- **not fully operationalized in the final validated workflow**

This is stronger and more accurate than either of these extremes:

- "RisingWave was neglected and is irrelevant"
- "RisingWave is already a core validated component of the final system"

## What Is True In The Current Repo

The repository still contains meaningful RisingWave support:

- deployment materials under `deps/risingwave/`
- configuration surface in `app-code/etl/config.py`
- a concrete ETL source adapter in `app-code/etl/io/sources/risingwave.py`

This means RisingWave is **not** merely a historical note. It remains a viable part of the intended Data Layer design.

## What Is Missing Today

What the current system does **not** yet provide is the surrounding operational layer needed to make RisingWave central to the final dissertation story:

- no main demo pipeline that depends on RisingWave
- no end-to-end orchestration path that clearly places RisingWave between ingestion and downstream consumers
- no utility layer for stream materialization lifecycle management
- no gateway or frontend surface that makes RisingWave visible as an active first-class subsystem
- no benchmark or evaluation path that specifically validates its contribution

So the gap is best described as an **operationalization gap**, not a conceptual gap.

## Recommended Report Framing

The report should describe RisingWave as:

- an intended streaming-state component of the **Data Layer**
- useful for maintaining continuously updated stream-derived state before downstream batch or agent consumption
- present in infrastructure and adapter form
- not yet fully integrated into the final validated execution path used in the dissertation's main demo and evaluation

## Claims That Are Safe

These are safe and accurate claims:

- RisingWave was part of the intended architecture
- RisingWave can still serve the originally intended purpose
- the project retains implementation scaffolding that would support its future integration
- the current omission is due to missing operational utilities and workflow integration, not because RisingWave is incompatible with the architecture

## Claims That Should Be Avoided

Avoid saying:

- RisingWave is a core validated component of the current end-to-end system
- the final demo proves the RisingWave-backed streaming architecture
- RisingWave is fully integrated into the current gateway/control/intelligence flow

Those statements would overstate what the current repo and evaluation actually demonstrate.

## Suggested Replacement Language

### Short Version

RisingWave remains a valid component within the intended Data Layer architecture, particularly for maintaining continuously updated streaming state before downstream batch or agent consumption. However, the present implementation does not yet provide the surrounding operational utilities, orchestration flows, and evaluation paths needed to make RisingWave part of the main validated execution path.

### More Formal Dissertation Version

RisingWave was originally intended to serve as a dedicated streaming-state and materialization layer within the Data Plane. This role remains architecturally valid, and the repository still retains deployment scaffolding, configuration support, and an ETL-facing source adapter for RisingWave. However, the final validated workflows in the present dissertation prioritize direct ingestion into Delta Lake and do not yet operationalize RisingWave as a first-class runtime dependency. The resulting gap is therefore not one of conceptual incompatibility, but of incomplete operational integration.

### Limitation Framing

Although the architecture allows for a dedicated streaming-state layer, the present prototype does not fully exercise RisingWave in the end-to-end workflow. In practice, the evaluated system path favors direct ingestion and persistence into Delta Lake, leaving RisingWave as a partially integrated capability rather than a fully validated subsystem.

### Future Work Framing

Future work should operationalize RisingWave more completely by adding utilities for materialized-view lifecycle management, orchestration support for downstream consumption, and benchmark paths that evaluate the benefit of an explicit streaming-state layer relative to the current direct-to-Delta workflow.

## Why It Was Not Fully Implemented Now

This is the strongest justification to keep in the report:

- the dissertation prioritized proving the integrated five-layer architecture
- the primary validated path focused on ETL durability, agent coordination, gateway access, and control-plane recovery
- fully integrating RisingWave would require:
  - additional orchestration logic
  - stream materialization utilities
  - downstream consumption patterns
  - revised evaluation and benchmarking methodology

That would have expanded the scope from "validate the unified architecture" into "fully productize a second streaming-state subsystem."

## Suggested Justification Paragraph

This integration was deferred to preserve dissertation scope. While RisingWave remains compatible with the intended Data Layer design, fully incorporating it into the final validated system would have required additional engineering around stream materialization management, downstream orchestration, and evaluation methodology. The present work instead prioritized establishing the core five-layer architecture and validating its integrated ETL, agent, gateway, and control-plane behavior.

## How To Position It Strategically

The best strategic position is:

- RisingWave is **not abandoned**
- RisingWave is **not fully realized**
- RisingWave is a **latent or partially integrated data-plane capability**

A useful phrase for the report is:

> RisingWave is present as a latent data-plane capability, but not yet fully operationalized in the final evaluated system path.

## Editing Guidance

- Do not erase RisingWave from the architecture if it was genuinely part of the intended design
- Do not present it as fully proven unless you add real workflow and evaluation support
- Prefer language such as:
  - "partially integrated"
  - "architecturally provisioned"
  - "not yet operationalized"
  - "deferred for scope reasons"

- Avoid language such as:
  - "fully implemented"
  - "core validated dependency"
  - "demonstrated in the final workflow"

## Optional Future Enhancement Directions

If you want to mention concrete next steps, the most credible ones are:

- RisingWave-backed materialized views for high-velocity stream state
- ETL utilities that read from RisingWave into downstream batch or agent workflows
- gateway visibility for stream-state resources
- benchmark comparison between direct-to-Delta ingestion and RisingWave-mediated stream processing
- control-plane observability hooks for streaming-state health
