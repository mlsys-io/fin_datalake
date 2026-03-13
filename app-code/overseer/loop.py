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
from overseer.collectors.base import BaseCollector
from overseer.config import load_config, load_endpoints
from overseer.models import (
    ActionType,
    ServiceEndpoint,
    ServiceMetrics,
    SystemSnapshot,
)
from overseer.policies import ActorHealthPolicy, KafkaLagPolicy
from overseer.policies.base import BasePolicy
from overseer.policies.cooldown import CooldownTracker
from overseer.actuators import AlertActuator, RayActuator, GatewayActuator
from overseer.actuators import BaseActuator
from overseer.store import MetricsStore


# ---------------------------------------------------------------------------
# Collector factory — maps service names to their Collector class
# ---------------------------------------------------------------------------

_COLLECTOR_REGISTRY: dict[str, type[BaseCollector]] = {
    "ray": RayCollector,
    "kafka": KafkaCollector,
    "prefect": PrefectCollector,
}


_POLICY_REGISTRY: dict[str, type[BasePolicy]] = {
    "kafka_lag": KafkaLagPolicy,
    "actor_health": ActorHealthPolicy,
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

        # Policies (Configurable)
        policy_names = overseer_cfg.get("policies", ["kafka_lag", "actor_health"])
        self.policies = []
        for name in policy_names:
            if name in _POLICY_REGISTRY:
                self.policies.append(_POLICY_REGISTRY[name]())
            else:
                logger.warning(f"Policy '{name}' not found in registry. Skipping.")

        # Actuators
        self.actuators: dict[str, BaseActuator] = {
            "ray": RayActuator(),
            "alert": AlertActuator(),
            "gateway": GatewayActuator(),
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
