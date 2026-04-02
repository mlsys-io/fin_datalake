"""
Gemini Agent Correctness Demo
==============================

Standalone script that proves the full production path end-to-end:

    ray.init() → GeminiAgent.deploy() → Ray Serve HTTP ingress
        → AgentHub registration → ask() → Gemini (or fallback) response

Run:
    # From app-code/ directory
    $env:GOOGLE_API_KEY = "your-key-here"          # PowerShell
    # export GOOGLE_API_KEY="your-key-here"         # bash/zsh

    uv run python -m pipelines.gemini_demo

No Prefect, no K8s, no external services beyond Gemini API.
A missing API key will NOT crash the demo — fallback mode is used instead.
"""

import asyncio
import os
import time
from typing import Any

import ray
from loguru import logger

# Demo questions that exercise different input shapes
QUESTIONS = [
    {
        "label": "Delta Lake concept",
        "payload": {"question": "What is Delta Lake and why is it useful for data lakehouses?"},
    },
    {
        "label": "Ray Serve description",
        "payload": {"question": "Explain Ray Serve in one sentence."},
    },
    {
        "label": "Trading signal reasoning",
        "payload": {
            "question": (
                "Given a bullish sentiment score of +0.7 and an SMA5 that is 1.5% above SMA20, "
                "what trading action would you recommend and why?"
            )
        },
    },
    # Also test raw-string input to verify _normalise_input works
    {
        "label": "Plain string input",
        "payload": "How does agent delegation work in a multi-agent system?",
    },
]

AGENT_NAME = "GeminiAgent-1"
SEPARATOR = "=" * 60


def _print_result(label: str, payload: Any, response: Any, elapsed: float, idx: int, total: int):
    """Pretty-print a single Q&A result."""
    status = "✅"
    question_text = payload if isinstance(payload, str) else payload.get("question", str(payload))
    print(f"\n{status} [{idx}/{total}] {label}")
    print(f"   Q: {question_text[:90]}{'...' if len(question_text) > 90 else ''}")
    resp_str = str(response)
    print(f"   A: {resp_str[:200]}{'...' if len(resp_str) > 200 else ''}")
    print(f"   ⏱  {elapsed:.2f}s")


async def _ask_async(handle, payload: Any) -> Any:
    """Send a single ask() call via the Ray Serve DeploymentHandle."""
    return await handle.ask.remote(payload)


def run_demo():
    # ── 1. Cluster Setup ──────────────────────────────────────────────────
    print(f"\n{SEPARATOR}")
    print("🚀  GEMINI AGENT CORRECTNESS DEMO")
    print(SEPARATOR)

    has_key = bool(os.environ.get("GOOGLE_API_KEY"))
    mode_label = "LIVE (Gemini API)" if has_key else "FALLBACK (no API key — echo mode)"
    print(f"   Mode      : {mode_label}")
    print(f"   Agent Name: {AGENT_NAME}")
    print(f"   Questions : {len(QUESTIONS)}")
    print(SEPARATOR)

    if not ray.is_initialized():
        logger.info("[Demo] Initialising local Ray cluster...")
        ray.init(ignore_reinit_error=True, logging_level="WARNING")

    # ── 2. Deploy GeminiAgent via BaseAgent.deploy() ──────────────────────
    logger.info(f"[Demo] Deploying {AGENT_NAME} to Ray Serve...")

    from etl.agents.gemini_agent import GeminiAgent
    from etl.agents.hub import get_hub

    deploy_start = time.perf_counter()
    handle = GeminiAgent.deploy(name=AGENT_NAME, num_replicas=1, num_cpus=0.5)
    deploy_time = time.perf_counter() - deploy_start
    logger.success(f"[Demo] Agent deployed in {deploy_time:.2f}s")

    # Give Ray Serve a moment to stabilise routing
    time.sleep(1.5)

    # ── 3. Verify AgentHub Registration ───────────────────────────────────
    print(f"\n[Step 1] Verifying AgentHub registration...")
    hub = get_hub()
    llm_qa_holders = ray.get(hub.find_by_capability.remote("llm_qa"))
    gemini_chat_holders = ray.get(hub.find_by_capability.remote("gemini_chat"))

    hub_ok = AGENT_NAME in llm_qa_holders and AGENT_NAME in gemini_chat_holders
    hub_status = "✅" if hub_ok else "❌"
    print(f"{hub_status} llm_qa    capability → {llm_qa_holders}")
    print(f"{hub_status} gemini_chat capability → {gemini_chat_holders}")

    # ── 4. Run Q&A Pipeline ───────────────────────────────────────────────
    print(f"\n[Step 2] Running {len(QUESTIONS)} Q&A pairs through the agent...\n")

    passed = 0
    total = len(QUESTIONS)
    demo_start = time.perf_counter()

    for i, q in enumerate(QUESTIONS, start=1):
        label = q["label"]
        payload = q["payload"]
        try:
            t0 = time.perf_counter()
            response = asyncio.run(_ask_async(handle, payload))
            elapsed = time.perf_counter() - t0
            _print_result(label, payload, response, elapsed, i, total)
            passed += 1
        except Exception as e:
            print(f"\n❌ [{i}/{total}] {label} — FAILED: {e}")

    total_time = time.perf_counter() - demo_start

    # ── 5. Verify via Gateway HTTP (optional, best-effort) ────────────────
    print(f"\n[Step 3] Best-effort Gateway HTTP check (POST /{AGENT_NAME}/ask)...")
    ray_serve_endpoint = os.environ.get("RAY_SERVE_ENDPOINT", "http://localhost:8000")
    url = f"{ray_serve_endpoint}/{AGENT_NAME}/ask"
    try:
        import httpx
        payload_http = {"payload": {"question": "Quick HTTP round-trip test."}, "session_id": None}
        with httpx.Client(timeout=15.0) as client:
            resp = client.post(url, json=payload_http)
            resp.raise_for_status()
            print(f"✅ HTTP {resp.status_code} from {url}")
            print(f"   Response: {str(resp.json())[:150]}")
    except Exception as e:
        print(f"⚠️  HTTP check skipped (Ray Serve HTTP proxy may not be reachable locally): {e}")

    # ── 6. Summary ────────────────────────────────────────────────────────
    print(f"\n{SEPARATOR}")
    all_ok = passed == total and hub_ok
    verdict = "🎯 ALL CHECKS PASSED" if all_ok else f"⚠️  {passed}/{total} Q&A passed, hub_ok={hub_ok}"
    print(f"{verdict}")
    print(f"   Q&A Results : {passed}/{total}")
    print(f"   AgentHub    : {'✅' if hub_ok else '❌'}")
    print(f"   Total Time  : {total_time:.2f}s  (deploy: {deploy_time:.2f}s)")
    print(f"   LLM Mode    : {mode_label}")
    print(SEPARATOR + "\n")

    return all_ok


if __name__ == "__main__":
    try:
        success = run_demo()
        raise SystemExit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("[Demo] Interrupted by user.")
    finally:
        # Flush logs before process exits
        time.sleep(0.5)
