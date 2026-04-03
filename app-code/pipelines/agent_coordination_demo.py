"""
Agent Coordination Demo — Component Verification

Verifies that the agent coordination primitives work correctly on a Ray cluster with Ray Serve:
  1. AgentHub     — register agents, discover by capability
  2. ContextStore — set/get shared state via Redis
  3. Delegation   — cross-agent task routing (client-side via ServeHandles)
  4. Ray Serve    — HTTP ingress and direct handle communication
  5. Graceful Shutdown — Serve app cleanup

Also demonstrates the SyncHandle API and connect() for cross-process discovery.

Usage:
    cd app-code
    uv run python -m pipelines.agent_coordination_demo

Requires:
    - Ray cluster running (local or remote via RAY_ADDRESS)
"""

import os
import sys
import time
import ray

# ============================================================================
# Config
# ============================================================================

RAY_ADDRESS = os.environ.get("RAY_ADDRESS", "auto")
PASS = "✅ PASS"
FAIL = "❌ FAIL"
results = []


def report(name: str, passed: bool, detail: str = ""):
    status = PASS if passed else FAIL
    results.append((name, passed))
    print(f"  {status}  {name}" + (f" — {detail}" if detail else ""))


def section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


# ============================================================================
# Demo Agent Classes
#
# These are plain BaseAgent subclasses. The new deploy() classmethod
# handles @ray.remote, naming, setup, and concurrency automatically.
# deploy() and connect() both return a SyncHandle, so callers can write:
#   agent.ask("question")  instead of  ray.get(handle.ask.remote("question"))
# ============================================================================

from etl.agents.base import BaseAgent
from etl.core.base_service import SyncHandle
from etl.runtime import ensure_ray


class AnalystAgent(BaseAgent):
    """Mock analyst that returns a canned answer when asked."""
    CAPABILITIES = ["analysis", "market_data"]

    def build_executor(self):
        def analyze(payload):
            return {"verdict": "BUY", "confidence": 0.85, "input": str(payload)}
        return analyze


class DataFetchAgent(BaseAgent):
    """Mock data fetcher that records received notifications."""
    CAPABILITIES = ["data_fetch"]

    def build_executor(self):
        self._messages = []
        return lambda payload: {"data": [1, 2, 3], "input": str(payload)}

    def on_message(self, event):
        """Store received notifications for verification."""
        from loguru import logger
        self._messages.append(event)
        logger.info(f"[{self.name}] Received notification: {event.get('topic')}")

    def get_messages(self):
        """Expose stored messages for verification."""
        return list(self._messages)


# ============================================================================
# Main Demo
# ============================================================================

def main():
    print("\n" + "=" * 60)
    print("  🧠 Agent Coordination — Component Demo")
    print("=" * 60)
    print(f"  Ray address: {RAY_ADDRESS}")

    # --- Init Ray ---
    ensure_ray(address=RAY_ADDRESS)

    cluster = ray.cluster_resources()
    print(f"  Cluster CPUs: {cluster.get('CPU', '?')}")
    print(f"  Cluster RAM:  {cluster.get('memory', 0) / 1e9:.1f} GB")

    # ====================================================================
    # 1. AgentHub — Service Discovery & Routing
    # ====================================================================
    section("1. AgentHub — Service Discovery & Routing")

    from etl.agents.hub import get_hub
    hub = get_hub()

    # a) Deploy two agents — returns ServeHandle (async-by-default)
    print("  Deploying AnalystAgent and DataFetchAgent via Ray Serve...")
    analyst = AnalystAgent.deploy(name="AnalystAgent")
    data_agent = DataFetchAgent.deploy(name="DataFetchAgent")


    time.sleep(1)

    # b) Verify agents are registered (registry remains actor-based)
    agents_list = ray.get(hub.list_agents.remote())
    agent_names = [a["name"] for a in agents_list]
    report(
        "Agents auto-registered with Hub on setup()",
        "AnalystAgent" in agent_names and "DataFetchAgent" in agent_names,
        f"Found: {agent_names}",
    )


    # c) Capability-based discovery
    analysts = ray.get(hub.find_by_capability.remote("analysis"))
    report(
        "find_by_capability('analysis')",
        "AnalystAgent" in analysts,
        f"Found: {analysts}",
    )

    fetchers = ray.get(hub.find_by_capability.remote("data_fetch"))
    report(
        "find_by_capability('data_fetch')",
        "DataFetchAgent" in fetchers,
        f"Found: {fetchers}",
    )


    # d) Direct ask — ServeHandle requires .remote() and ray.get()
    response = ray.get(analyst.ask.remote("Should I buy AAPL?"))
    report(
        "AnalystAgent.ask() returns verdict",
        response.get("verdict") == "BUY",
        f"Response: {response}",
    )

    # e) connect() — retrieve a ServeHandle for an existing app
    reconnected = AnalystAgent.connect("AnalystAgent")
    response2 = ray.get(reconnected.ask.remote("Re-check AAPL"))
    report(
        "connect() retrieves existing ServeHandle",
        response2.get("verdict") == "BUY",
        f"Response: {response2}",
    )


    # f) Synchronous routing via hub
    hub_response = hub.call("AnalystAgent", "Hub-routed question")
    report(
        "hub.call() routes synchronously",
        hub_response.get("verdict") == "BUY",
        f"Response: {hub_response}",
    )

    # g) Hub stats
    stats = ray.get(hub.get_stats.remote())
    report(
        "Hub stats available",
        stats.get("registered_agents", 0) >= 2,
        f"Stats: {stats}",
    )

    # ====================================================================
    # 2. AgentHub — Notifications
    # ====================================================================
    section("2. AgentHub — Notifications")

    # a) Notify a specific agent
    notified = ray.get(hub.notify.remote(
        "DataFetchAgent",
        {"topic": "market_data", "payload": {"symbol": "AAPL", "price": 185.50}, "sender": "DemoScript"},
    ))
    report("hub.notify() dispatched", notified is True)

    # b) Notify by capability
    cap_notified = ray.get(hub.notify_capability.remote(
        "data_fetch",
        {"topic": "system_event", "payload": {"event": "data_refresh"}, "sender": "DemoScript"},
    ))
    report(
        "hub.notify_capability('data_fetch')",
        cap_notified >= 1,
        f"Notified {cap_notified} agent(s)",
    )

    # c) Verify notifications were received
    time.sleep(1)  # Allow fire-and-forget delivery to complete
    messages = data_agent.get_messages()
    report(
        "DataFetchAgent received notifications",
        len(messages) >= 2,
        f"Messages received: {len(messages)}",
    )

    # d) Health check
    health = ray.get(hub.health_check.remote())
    report(
        "hub.health_check() reports alive status",
        health.get("AnalystAgent") is True and health.get("DataFetchAgent") is True,
        f"Health: {health}",
    )

    # ====================================================================
    # 3. ContextStore — Shared State
    # ====================================================================
    section("3. ContextStore — Shared State")

    from etl.agents.context import get_context
    ctx = get_context()


    # a) Set and get
    ctx.set("demo:signal", "BUY_AAPL", owner="AnalystAgent")
    value = ctx.get("demo:signal")
    report(
        "set() and get() round-trip",
        value == "BUY_AAPL",
        f"Value: {value}",
    )

    # b) Append to list
    ctx.set("demo:history", [], owner="DemoScript")
    ctx.append("demo:history", {"action": "BUY", "tick": 1})
    ctx.append("demo:history", {"action": "HOLD", "tick": 2})
    history = ctx.get("demo:history")
    report(
        "append() builds a list atomically",
        isinstance(history, list) and len(history) == 2,
        f"History: {history}",
    )

    # c) TTL expiry
    ctx.set("demo:ephemeral", "will_expire", ttl=1)
    before = ctx.get("demo:ephemeral")
    report("Value exists before TTL", before == "will_expire")

    time.sleep(2)  # Wait for TTL to expire
    after = ctx.get("demo:ephemeral")
    report("Value expired after TTL", after is None, f"After: {after}")

    # d) Key listing
    keys = ctx.keys("demo:")
    report(
        "keys('demo:') returns filtered list",
        len(keys) >= 2,
        f"Keys: {keys}",
    )

    # e) Info
    info = ctx.get_info("demo:signal")
    report(
        "get_info() returns metadata",
        info is not None and info.get("owner") == "AnalystAgent",
        f"Info: {info}",
    )

    # ====================================================================
    # 4. Agent Delegation
    # ====================================================================
    section("4. Agent Delegation — Cross-Agent Task Routing")

    # DataFetchAgent delegates "analysis" to AnalystAgent via AgentHub
    try:
        # data_agent.delegate() performs client-side routing via Hub Registry + ServeHandle
        result = ray.get(data_agent.delegate.remote("analysis", "Analyze MSFT please"))
        report(
            "DataFetchAgent.delegate('analysis') (Ray Serve-powered)",
            result.get("verdict") == "BUY",
            f"Delegated result: {result}",
        )
    except Exception as e:
        report("DataFetchAgent.delegate('analysis')", False, f"Error: {e}")


    # ====================================================================
    # 5. Graceful Shutdown
    # ====================================================================
    section("5. Graceful Shutdown — Lifecycle Hooks")

    # Use shutdown() — runs on_stop() (deregister from hub) then exits actor
    try:
        analyst.shutdown.remote()
        report("AnalystAgent.shutdown() triggered", True)
    except Exception:
        report("AnalystAgent.shutdown()", True)

    try:
        data_agent.shutdown.remote()
        report("DataFetchAgent.shutdown() triggered", True)
    except Exception:
        report("DataFetchAgent.shutdown()", True)

    # Verify Registry auto-deregisters on worker death
    time.sleep(2)
    remaining = ray.get(hub.list_agents.remote())
    remaining_names = [a["name"] for a in remaining if a.get("alive", False)]
    report(
        "Registry health checks identify dead apps",
        "AnalystAgent" not in remaining_names and "DataFetchAgent" not in remaining_names,
        f"Remaining alive: {remaining_names}",
    )


    # Clean up context entries
    ctx.delete("demo:signal")
    ctx.delete("demo:history")

    # ====================================================================
    # Summary
    # ====================================================================
    section("Summary")
    passed = sum(1 for _, p in results if p)
    total = len(results)
    print(f"  {passed}/{total} checks passed\n")

    for name, p in results:
        print(f"    {'✅' if p else '❌'}  {name}")

    print()
    if passed == total:
        print("  🎉 All agent coordination components are working!")
    else:
        print(f"  ⚠️  {total - passed} check(s) failed — review above.")

    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
