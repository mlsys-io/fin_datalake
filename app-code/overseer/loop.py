"""
Overseer Core Loop — The MAPE-K engine.

Orchestrates: Collectors → Policies → Actuators in a continuous async loop.
Completely standalone from Ray — connects to services via their external APIs.
"""

from __future__ import annotations

import asyncio
from typing import Optional

from loguru import logger

from overseer.collectors import (
    GenericHealthCollector,
    KafkaCollector,
    PrefectCollector,
    RayCollector,
)
from overseer.collectors.delta_lake import DeltaLakeCollector
from overseer.collectors.base import BaseCollector
from overseer.config import load_config, load_endpoints
from overseer.models import (
    ActionType,
    OverseerAction,
    ServiceEndpoint,
    ServiceMetrics,
    SystemSnapshot,
)
from overseer.policies.base import BasePolicy
from overseer.policies.cooldown import CooldownTracker
from overseer.actuators import AlertActuator, RayActuator, GatewayActuator, StatusReporterActuator
from overseer.actuators.webhook import WebhookActuator
from overseer.actuators import BaseActuator
from overseer.store import MetricsStore
from overseer.s3_sync import sync_policies
from overseer.hot_reload import load_custom_policies


# ---------------------------------------------------------------------------
# Collector factory — maps service names to their Collector class
# ---------------------------------------------------------------------------

_COLLECTOR_REGISTRY: dict[str, type[BaseCollector]] = {
    "ray": RayCollector,
    "kafka": KafkaCollector,
    "prefect": PrefectCollector,
    "delta_lake": DeltaLakeCollector,
}


def build_collector(endpoint: ServiceEndpoint) -> BaseCollector:
    """
    Build the appropriate Collector for a given ServiceEndpoint.

    If no specialized collector exists, fall back to GenericHealthCollector.
    This is the extensibility point: register new collectors here.
    """
    cls = _COLLECTOR_REGISTRY.get(endpoint.name, GenericHealthCollector)
    return cls(endpoint)


# ---------------------------------------------------------------------------
# The Overseer
# ---------------------------------------------------------------------------

class Overseer:
    """
    Standalone autonomic monitoring service.

    Implements the MAPE-K control loop:
      Monitor (Collectors) → Analyze/Plan (Policies) → Execute (Actuators)
    with a shared Knowledge base (MetricsStore).
    """

    def __init__(self, config_path: Optional[str] = None):
        # Load configuration
        raw_config = load_config(config_path)
        endpoints = load_endpoints(config_path)
        overseer_cfg = raw_config.get("overseer", {})

        self.loop_interval = overseer_cfg.get("loop_interval_seconds", 15)
        self.store = MetricsStore(
            max_snapshots=overseer_cfg.get("metrics_history_size", 200),
        )
        self.cooldown = CooldownTracker(
            cooldown_seconds=overseer_cfg.get("cooldown_seconds", 120)
        )

        # Build collectors from config
        self.collectors = [build_collector(ep) for ep in endpoints]

        # ----------------------------------------------------------------------- #
        # PURE DYNAMIC POLICIES: 0 built-in policies at startup.
        # Everything is pulled from S3.
        # ----------------------------------------------------------------------- #
        self.policies: list[BasePolicy] = []

        # Custom Policies Sync
        self.custom_policy_cfg = overseer_cfg.get("custom_policies", {})
        self.s3_uri = self.custom_policy_cfg.get("s3_uri", "s3://demo-lake/overseer-policies/")
        self.local_custom_dir = self.custom_policy_cfg.get("local_dir", "/tmp/overseer_custom_policies/")
        self.sync_interval_seconds = self.custom_policy_cfg.get("sync_interval_seconds", 60)
        self.last_sync_time = 0.0

        # Actuators
        self.actuators: dict[str, BaseActuator] = {
            "ray": RayActuator(),
            "alert": AlertActuator(),
            "gateway": GatewayActuator(),
            "reporter": StatusReporterActuator(self.store),
            "webhook": WebhookActuator(webhook_url=overseer_cfg.get("webhook_url")),
        }

        logger.info(
            f"Overseer initialized: {len(self.collectors)} collectors, "
            f"{len(self.policies)} policies, {len(self.actuators)} actuators. "
            f"Loop interval: {self.loop_interval}s"
        )

    async def run(self) -> None:
        """Main control loop — runs forever."""
        logger.info("Overseer control loop starting...")
        cycle = 0
        while True:
            cycle += 1
            logger.info(f"--- Cycle {cycle} ---")

            # --- HOT RELOAD CUSTOM POLICIES ---
            current_time = asyncio.get_event_loop().time()
            if current_time - self.last_sync_time >= self.sync_interval_seconds:
                self.last_sync_time = current_time
                try:
                    await asyncio.to_thread(sync_policies, self.s3_uri, self.local_custom_dir)
                    self.policies = await asyncio.to_thread(load_custom_policies, self.local_custom_dir)
                    
                    if self.policies:
                        logger.info(f"Active policies: {len(self.policies)} (100% dynamically loaded from S3).")
                except Exception as e:
                    logger.error(f"Failed to hot-reload custom policies: {e}")

            # 1. MONITOR — probe all services concurrently
            snapshot = SystemSnapshot()
            results = await asyncio.gather(
                *[self._safe_collect(c) for c in self.collectors],
                return_exceptions=True,
            )
            for collector, result in zip(self.collectors, results):
                name = collector.endpoint.name
                if isinstance(result, Exception):
                    snapshot.services[name] = ServiceMetrics(
                        service=name, healthy=False, error=str(result),
                    )
                else:
                    snapshot.services[name] = result

            await self.store.append_snapshot(snapshot)

            # Log health status
            for name, metrics in snapshot.services.items():
                status = "✅" if metrics.healthy else "❌"
                logger.info(f"  {status} {name}: healthy={metrics.healthy}")

            # 2. ANALYZE + PLAN — run policies
            actions = []
            for policy in self.policies:
                try:
                    actions.extend(policy.evaluate(snapshot))
                except Exception as e:
                    logger.error(f"Policy {policy.__class__.__name__} failed: {e}")

            if actions:
                logger.info(f"  📋 {len(actions)} action(s) planned")
            else:
                logger.debug("  No actions needed")

            # 3. EXECUTE — perform actions
            for action in actions:
                # Cooldown check
                if not self.cooldown.can_fire(action.type.value, action.target):
                    logger.info(f"  ⏳ Skipping {action.type.value} — cooldown active")
                    continue

                logger.info(f"  🔧 Executing: {action.type.value} — {action.reason}")
                await self.store.append_alert(action)

                # Every action also goes through the AlertActuator for logging
                await self.actuators["alert"].execute(action)

                if action.type in (ActionType.RESPAWN, ActionType.SCALE_UP, ActionType.ALERT) and "webhook" in self.actuators:
                    await self.actuators["webhook"].execute(action)

                # Route to the appropriate actuator
                actuator = self.actuators.get(action.target)
                if actuator and action.target != "alert":
                    try:
                        # Prevent rogue actuators from starving the Overseer loop
                        result = await asyncio.wait_for(
                            actuator.execute(action), 
                            timeout=30.0
                        )
                        if result.success:
                            self.cooldown.record(action.type.value, action.target)
                        else:
                            logger.error(f"  Actuator failed: {result.error}")
                    except asyncio.TimeoutError:
                        logger.error(f"  Actuator {action.target} timed out after 30s.")
                    except Exception as e:
                        logger.error(f"  Actuator {action.target} crashed: {e}")

            # 4. REPORT — Generate living documentation
            await self.actuators["reporter"].execute(
                OverseerAction(type=ActionType.ALERT, target="reporter", reason="System status heartbeat")
            )

            await asyncio.sleep(self.loop_interval)

    async def _safe_collect(self, collector: BaseCollector) -> ServiceMetrics:
        """Run a single collector with error handling."""
        try:
            return await collector.collect()
        except Exception as e:
            return ServiceMetrics(
                service=collector.endpoint.name,
                healthy=False,
                error=str(e),
            )
