"""Validate Overseer self-healing against the current Ray Serve agent model."""

from __future__ import annotations

import time

import ray
import ray.serve as serve
from loguru import logger
from ray.util.state import list_actors

from agents.dummy_agents import SupportAgent
from etl.agents.hub import get_hub
from etl.runtime import ensure_ray, resolve_ray_namespace, resolve_serve_response


def _find_replica_actor_name(app_name: str) -> str | None:
    prefix = f"ServeReplica:{app_name}:"
    try:
        actors = list_actors(filters=[("state", "=", "ALIVE")], detail=True)
    except Exception as e:
        logger.warning(f"Failed to query Ray state API: {e}")
        return None

    for actor in actors:
        name = str(actor.get("name") or "")
        if name.startswith(prefix):
            return name
    return None


def _kill_serve_actor(app_name: str) -> bool:
    """Kill a Serve replica so the Overseer can observe a dead actor."""
    actor_name = _find_replica_actor_name(app_name)
    namespace = resolve_ray_namespace()

    if actor_name:
        try:
            actor = ray.get_actor(actor_name, namespace=namespace)
            ray.kill(actor, no_restart=True)
            logger.warning(f"Killed Serve replica '{actor_name}' (no_restart=True).")
            return True
        except Exception as e:
            logger.warning(f"Failed to kill replica '{actor_name}': {e}")

    try:
        serve.delete(app_name, _blocking=True)
        logger.warning(f"Deleted Serve app '{app_name}' as fallback chaos action.")
        return True
    except Exception as e:
        logger.error(f"Fallback serve.delete() failed: {e}")
        return False


def run_self_healing_demo() -> None:
    """
    Deploy a dummy SupportAgent, kill its Serve replica, and wait for the
    Overseer to restore the same SupportAgent deployment via RayActuator.
    """
    logger.info("=== OVERSEER SELF-HEALING DEMO ===")

    ensure_ray()
    hub = get_hub()

    victim_name = "SupportAgent-Victim1"
    capability_id = "chat.support.respond"

    logger.info(f"[1] Deploying {victim_name}...")
    SupportAgent.deploy(name=victim_name)
    time.sleep(2)

    handle = serve.get_app_handle(victim_name)
    response = resolve_serve_response(handle.chat.remote("Gateway returned 503"))
    logger.success(f"[1] Victim agent is responding: {response}")

    logger.warning(f"[2] Killing Serve replica for {victim_name}...")
    if not _kill_serve_actor(victim_name):
        raise RuntimeError("Could not kill the victim agent.")

    time.sleep(1)
    try:
        resolve_serve_response(handle.chat.remote("Ping?"))
        logger.warning("Victim still responded after kill; recovery may be harder to observe.")
    except Exception:
        logger.success("[2] Confirmed: victim agent is no longer serving requests.")

    logger.info("[3] Waiting for Overseer recovery of the same deployment name...")
    start = time.perf_counter()

    for attempt in range(45):
        time.sleep(1)
        if attempt % 5 == 0:
            logger.debug(f"... polling AgentHub (attempt {attempt + 1}/45)")

        agent_names = ray.get(hub.find_by_capability.remote(capability_id))
        if victim_name not in agent_names:
            continue

        try:
            healed_handle = serve.get_app_handle(victim_name)
            healed_response = resolve_serve_response(
                healed_handle.chat.remote("Hello from healed agent")
            )
            mttr = time.perf_counter() - start
            logger.success(f"[4] Recovery successful in {mttr:.1f}s via {victim_name}")
            logger.success(f"Recovered same-name deployment response: {healed_response}")
            return
        except Exception:
            continue

    raise RuntimeError("Recovery timed out. Check Overseer logs and Ray actuator wiring.")


if __name__ == "__main__":
    try:
        run_self_healing_demo()
    except KeyboardInterrupt:
        logger.info("Demo interrupted.")
