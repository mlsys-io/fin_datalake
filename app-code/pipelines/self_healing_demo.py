"""Validate Overseer self-healing against the current Ray Serve deployment model."""

from __future__ import annotations

import time
from uuid import uuid4

import ray.serve as serve
from loguru import logger

from agents.dummy_agents import SupportAgent
from etl.agents.catalog import delete_agent_catalog_entry, list_agent_catalog_entries
from etl.runtime import ensure_ray, resolve_serve_response


POLL_INTERVAL_SECONDS = 2
RECOVERY_TIMEOUT_SECONDS = 120
STATE_LOG_EVERY = 5


def _victim_name() -> str:
    return f"SupportAgent-Victim-{uuid4().hex[:8]}"


def _get_catalog_entry(name: str) -> dict | None:
    for entry in list_agent_catalog_entries(runtime_source="ray-serve", enabled_only=False):
        if str(entry.get("name") or "").strip() == name:
            return entry
    return None


def _delete_serve_app(app_name: str) -> bool:
    """Delete the Serve app to simulate a deployment outage."""
    try:
        serve.delete(app_name, _blocking=True)
        logger.warning(f"Deleted Serve app '{app_name}' to simulate an outage.")
        return True
    except Exception as e:
        logger.error(f"serve.delete('{app_name}') failed: {e}")
        return False


def _cleanup_victim(app_name: str) -> None:
    try:
        serve.delete(app_name, _blocking=True)
        logger.info(f"Cleanup removed Serve app '{app_name}'.")
    except Exception as e:
        logger.debug(f"Cleanup could not delete Serve app '{app_name}': {e}")

    try:
        removed = delete_agent_catalog_entry(app_name)
        if removed:
            logger.info(f"Cleanup removed durable catalog entry '{app_name}'.")
    except Exception as e:
        logger.debug(f"Cleanup could not delete catalog entry '{app_name}': {e}")


def run_self_healing_demo() -> None:
    """
    Deploy a dummy SupportAgent, delete the Serve app to simulate an outage,
    and wait for the Overseer to restore the same deployment name.
    """
    logger.info("=== OVERSEER SELF-HEALING DEMO ===")

    ensure_ray()
    victim_name = _victim_name()
    saw_non_ready_state = False
    start = time.perf_counter()

    try:
        logger.info(f"[1] Deploying {victim_name}...")
        SupportAgent.deploy(name=victim_name)
        time.sleep(2)

        handle = serve.get_app_handle(victim_name)
        response = resolve_serve_response(handle.chat.remote("Gateway returned 503"))
        logger.success(f"[1] Victim deployment is responding: {response}")

        logger.warning(f"[2] Deleting Serve app for {victim_name} to simulate outage...")
        if not _delete_serve_app(victim_name):
            raise RuntimeError("Could not simulate the victim outage.")

        time.sleep(1)
        try:
            resolve_serve_response(handle.chat.remote("Ping?"))
            logger.warning("Victim still responded after deletion; Serve teardown may still be in progress.")
        except Exception:
            logger.success("[2] Confirmed: victim deployment is no longer serving requests.")

        logger.info("[3] Waiting for Overseer recovery of the same deployment name...")
        attempts = RECOVERY_TIMEOUT_SECONDS // POLL_INTERVAL_SECONDS
        for attempt in range(attempts):
            time.sleep(POLL_INTERVAL_SECONDS)
            entry = _get_catalog_entry(victim_name)

            observed_status = str(entry.get("observed_status") or "unknown") if entry else "missing"
            health_status = str(entry.get("health_status") or "unknown") if entry else "unknown"
            recovery_state = str(entry.get("recovery_state") or "unknown") if entry else "unknown"
            failure_reason = entry.get("last_failure_reason") if entry else None

            if observed_status in {"missing", "recovering", "degraded", "offline"}:
                saw_non_ready_state = True

            if attempt % STATE_LOG_EVERY == 0:
                logger.info(
                    f"[poll] {victim_name} status={observed_status} "
                    f"health={health_status} recovery={recovery_state} "
                    f"reason={failure_reason or '-'}"
                )

            if observed_status != "ready":
                continue

            try:
                healed_handle = serve.get_app_handle(victim_name)
                healed_response = resolve_serve_response(
                    healed_handle.chat.remote("Hello from healed agent")
                )
                if not saw_non_ready_state:
                    raise RuntimeError(
                        "Recovery reached 'ready' without ever observing a non-ready outage state."
                    )
                mttr = time.perf_counter() - start
                logger.success(f"[4] Recovery successful in {mttr:.1f}s via {victim_name}")
                logger.success(f"Recovered same-name deployment response: {healed_response}")
                return
            except Exception as exc:
                if attempt % STATE_LOG_EVERY == 0:
                    logger.debug(
                        f"[poll] {victim_name} catalog is ready, but Serve handle is not yet usable: {exc}"
                    )

        entry = _get_catalog_entry(victim_name)
        raise RuntimeError(
            "Recovery timed out. "
            f"Last observed state: observed={entry.get('observed_status') if entry else 'missing'}, "
            f"health={entry.get('health_status') if entry else 'unknown'}, "
            f"recovery={entry.get('recovery_state') if entry else 'unknown'}, "
            f"reason={entry.get('last_failure_reason') if entry else 'catalog entry missing'}."
        )
    finally:
        logger.info(f"[cleanup] Cleaning up victim deployment '{victim_name}'...")
        _cleanup_victim(victim_name)


if __name__ == "__main__":
    try:
        run_self_healing_demo()
    except KeyboardInterrupt:
        logger.info("Demo interrupted.")
