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
- Airflow-style baseline: batch orchestration pattern without native persistent agents
- LangGraph: agent coordination comparison only, without the full infrastructure stack

Suggested comparison dimensions:

- distributed ETL
- real-time streaming
- persistent agents
- delegation and shared state
- self-healing
- unified API surface

## Recommended Operator Scripts

- `scripts/deploy_test_agents.py`
- `pipelines/self_healing_demo.py`
- `etl-agents deploy-baseline`
- `etl-agents list`

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
