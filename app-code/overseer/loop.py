"""
Overseer Core Loop - The MAPE-K engine.

Orchestrates: Collectors -> Policies -> Actuators in a continuous async loop.
Completely standalone from Ray - connects to services via their external APIs.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Optional

from loguru import logger

from etl.agents.catalog import list_agent_catalog_entries, update_agent_catalog_status
from overseer.actuators import (
    AlertActuator,
    BaseActuator,
    GatewayActuator,
    RayActuator,
    StatusReporterActuator,
)
from overseer.actuators.webhook import WebhookActuator
from overseer.collectors import (
    GenericHealthCollector,
    KafkaCollector,
    PrefectCollector,
    RayCollector,
)
from overseer.collectors.base import BaseCollector
from overseer.collectors.delta_lake import DeltaLakeCollector
from overseer.config import load_config, load_endpoints
from overseer.hot_reload import load_custom_policies
from overseer.models import (
    ActionResult,
    ActionType,
    OverseerAction,
    ServiceEndpoint,
    ServiceMetrics,
    SystemSnapshot,
)
from overseer.policies.base import BasePolicy
from overseer.policies.cooldown import CooldownTracker
from overseer.policies.healing import ActorHealthPolicy
from overseer.policies.scaling import KafkaLagPolicy
from overseer.s3_sync import sync_policies
from overseer.store import MetricsStore


_COLLECTOR_REGISTRY: dict[str, type[BaseCollector]] = {
    "ray": RayCollector,
    "kafka": KafkaCollector,
    "prefect": PrefectCollector,
    "delta_lake": DeltaLakeCollector,
}


def build_collector(endpoint: ServiceEndpoint) -> BaseCollector:
    """Build the collector for a given endpoint, falling back to generic health."""
    cls = _COLLECTOR_REGISTRY.get(endpoint.name, GenericHealthCollector)
    return cls(endpoint)


class Overseer:
    """
    Standalone autonomic monitoring service.

    Implements the MAPE-K control loop:
      Monitor (Collectors) -> Analyze/Plan (Policies) -> Execute (Actuators)
    with a shared Knowledge base (MetricsStore).
    """

    def __init__(self, config_path: Optional[str] = None):
        raw_config = load_config(config_path)
        endpoints = load_endpoints(config_path)
        overseer_cfg = raw_config.get("overseer", {})

        self.loop_interval = overseer_cfg.get("loop_interval_seconds", 15)
        self.respawn_recovery_timeout_seconds = float(
            overseer_cfg.get("respawn_recovery_timeout_seconds", 60)
        )
        self.store = MetricsStore(
            max_snapshots=overseer_cfg.get("metrics_history_size", 200),
        )
        self.cooldown = CooldownTracker(
            cooldown_seconds=overseer_cfg.get("cooldown_seconds", 120)
        )

        self.collectors = [build_collector(ep) for ep in endpoints]

        self.builtin_policies = self._build_builtin_policies(overseer_cfg)
        self.policies: list[BasePolicy] = list(self.builtin_policies)

        self.custom_policy_cfg = overseer_cfg.get("custom_policies", {})
        self.custom_policies_enabled = self.custom_policy_cfg.get("enabled", True)
        self.s3_uri = self.custom_policy_cfg.get(
            "s3_uri", "s3://demo-lake/overseer-policies/"
        )
        self.local_custom_dir = self.custom_policy_cfg.get(
            "local_dir", "/tmp/overseer_custom_policies/"
        )
        self.sync_interval_seconds = self.custom_policy_cfg.get(
            "sync_interval_seconds", 60
        )
        self.last_sync_time = 0.0

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
            f"Loop interval: {self.loop_interval}s, "
            f"Respawn recovery timeout: {self.respawn_recovery_timeout_seconds}s"
        )

    async def run(self) -> None:
        """Main control loop - runs forever."""
        logger.info("Overseer control loop starting...")
        cycle = 0
        while True:
            cycle += 1
            logger.info(f"--- Cycle {cycle} ---")

            current_time = asyncio.get_event_loop().time()
            if (
                self.custom_policies_enabled
                and current_time - self.last_sync_time >= self.sync_interval_seconds
            ):
                self.last_sync_time = current_time
                try:
                    await asyncio.to_thread(
                        sync_policies, self.s3_uri, self.local_custom_dir
                    )
                    custom_policies = await asyncio.to_thread(
                        load_custom_policies, self.local_custom_dir
                    )
                    self.policies = [*self.builtin_policies, *custom_policies]

                    if custom_policies:
                        logger.info(
                            f"Active policies: {len(self.policies)} total "
                            f"({len(self.builtin_policies)} built-in, "
                            f"{len(custom_policies)} custom)."
                        )
                except Exception as exc:
                    logger.error(f"Failed to hot-reload custom policies: {exc}")

            snapshot = SystemSnapshot()
            results = await asyncio.gather(
                *[self._safe_collect(c) for c in self.collectors],
                return_exceptions=True,
            )
            for collector, result in zip(self.collectors, results):
                name = collector.endpoint.name
                if isinstance(result, Exception):
                    snapshot.services[name] = ServiceMetrics(
                        service=name,
                        healthy=False,
                        error=str(result),
                    )
                else:
                    snapshot.services[name] = result

            agent_control_metrics = await asyncio.to_thread(
                self._build_agent_control_metrics, snapshot
            )
            snapshot.services[agent_control_metrics.service] = agent_control_metrics
            await asyncio.to_thread(
                self._sync_agent_catalog_state,
                agent_control_metrics.data.get("deployments", []),
            )

            await self.store.append_snapshot(snapshot)

            for name, metrics in snapshot.services.items():
                status = "[OK]" if metrics.healthy else "[WARN]"
                logger.info(f"  {status} {name}: healthy={metrics.healthy}")

            actions: list[OverseerAction] = []
            for policy in self.policies:
                try:
                    actions.extend(policy.evaluate(snapshot))
                except Exception as exc:
                    logger.error(f"Policy {policy.__class__.__name__} failed: {exc}")

            if actions:
                logger.info(f"  [PLAN] {len(actions)} action(s) planned")
            else:
                logger.debug("  No actions needed")

            for action in actions:
                cooldown_scope = self._cooldown_scope(action)
                use_generic_cooldown = action.type != ActionType.RESPAWN
                if (
                    use_generic_cooldown
                    and not self.cooldown.can_fire(action.type.value, cooldown_scope)
                ):
                    logger.info(
                        f"  [SKIP] {action.type.value} - cooldown active for {cooldown_scope}"
                    )
                    continue

                logger.info(f"  [EXEC] {action.type.value} - {action.reason}")
                await self._mark_action_started(action)
                await self.actuators["alert"].execute(action)

                if (
                    action.type in (ActionType.RESPAWN, ActionType.SCALE_UP, ActionType.ALERT)
                    and "webhook" in self.actuators
                ):
                    await self.actuators["webhook"].execute(action)

                result = ActionResult(success=True, detail="No-op alert recorded")
                actuator = self.actuators.get(action.target)
                if actuator and action.target != "alert":
                    try:
                        result = await asyncio.wait_for(
                            actuator.execute(action),
                            timeout=30.0,
                        )
                        if not result.success:
                            logger.error(f"  Actuator failed: {result.error}")
                    except asyncio.TimeoutError:
                        result = ActionResult(
                            success=False,
                            error=f"Actuator {action.target} timed out after 30s.",
                        )
                        logger.error(f"  {result.error}")
                    except Exception as exc:
                        result = ActionResult(success=False, error=str(exc))
                        logger.error(f"  Actuator {action.target} crashed: {exc}")

                if use_generic_cooldown:
                    self.cooldown.record(action.type.value, cooldown_scope)
                await self._apply_action_result(action, result)
                await self.store.append_alert(self._build_action_alert(action, result))

            await self.actuators["reporter"].execute(
                OverseerAction(
                    type=ActionType.ALERT,
                    target="reporter",
                    reason="System status heartbeat",
                )
            )

            await asyncio.sleep(self.loop_interval)

    def _build_builtin_policies(self, overseer_cfg: dict) -> list[BasePolicy]:
        """Build the built-in policy set used for demos and fallback operation."""
        builtin_cfg = overseer_cfg.get("builtin_policies", {})
        enabled = builtin_cfg.get("enabled", True)
        if not enabled:
            return []

        policies: list[BasePolicy] = []

        if builtin_cfg.get("actor_health_enabled", True):
            policies.append(ActorHealthPolicy())

        if builtin_cfg.get("kafka_lag_enabled", True):
            policies.append(
                KafkaLagPolicy(
                    scale_up_threshold=builtin_cfg.get("kafka_scale_up_threshold", 500),
                    scale_up_count=builtin_cfg.get("kafka_scale_up_count", 1),
                    scale_down_idle_threshold=builtin_cfg.get(
                        "kafka_scale_down_idle_threshold", 0
                    ),
                    scale_down_count=builtin_cfg.get("kafka_scale_down_count", 1),
                    agent_class=builtin_cfg.get("kafka_agent_class", "SentimentAgent"),
                )
            )

        return policies

    def _build_agent_control_metrics(self, snapshot: SystemSnapshot) -> ServiceMetrics:
        catalog_entries = list_agent_catalog_entries(
            runtime_source="ray-serve",
            enabled_only=True,
        )
        ray_metrics = snapshot.services.get("ray")
        ray_available = bool(ray_metrics and ray_metrics.healthy)
        serve_applications = {}
        if ray_metrics:
            serve_applications = dict(ray_metrics.data.get("serve_applications_by_name") or {})
            if not serve_applications:
                serve_applications = {
                    str(app.get("name") or "").strip(): dict(app)
                    for app in ray_metrics.data.get("serve_applications", [])
                    if str(app.get("name") or "").strip()
                }

        deployments = []
        summary = {
            "total": 0,
            "managed": 0,
            "ready": 0,
            "degraded": 0,
            "missing": 0,
            "recovering": 0,
            "offline": 0,
            "stale": 0,
            "unknown": 0,
        }

        for entry in catalog_entries:
            metadata = dict(entry.get("metadata") or {})
            deployment_name = str(metadata.get("app_name") or entry.get("name") or "").strip()
            deployment = self._normalize_catalog_deployment(
                entry=entry,
                ray_available=ray_available,
                app_state=serve_applications.get(deployment_name, {}),
            )
            deployments.append(deployment)
            summary["total"] += 1
            if deployment.get("managed_by_overseer"):
                summary["managed"] += 1
            observed_status = str(deployment.get("observed_status") or "unknown")
            summary[observed_status] = summary.get(observed_status, 0) + 1

        healthy = self._is_agent_control_healthy(deployments, ray_available)
        detail = None
        if not ray_available:
            detail = "Ray runtime is unavailable; deployment states are catalog-driven only."

        return ServiceMetrics(
            service="agent_control",
            healthy=healthy,
            data={
                "deployments": deployments,
                "summary": summary,
                "ray_available": ray_available,
            },
            error=detail,
        )

    def _normalize_catalog_deployment(
        self,
        *,
        entry: dict[str, Any],
        ray_available: bool,
        app_state: dict[str, Any],
    ) -> dict[str, Any]:
        metadata = dict(entry.get("metadata") or {})
        deployment_metadata = dict(entry.get("deployment_metadata") or {})
        deployment_name = str(metadata.get("app_name") or entry.get("name") or "").strip()
        desired_status = str(entry.get("desired_status") or "running")
        previous_observed = str(entry.get("observed_status") or "unknown")
        previous_health = str(entry.get("health_status") or "unknown")
        previous_recovery = str(entry.get("recovery_state") or "idle")
        previous_action = str(entry.get("last_action_type") or "").strip().lower()
        managed = bool(entry.get("managed_by_overseer", True))
        in_respawn_recovery_window = self._in_respawn_recovery_window(entry)
        respawn_recovery_active = (
            previous_recovery == "recovering"
            and previous_action == ActionType.RESPAWN.value
            and in_respawn_recovery_window
        )
        respawn_recovery_timed_out = (
            previous_recovery == "recovering"
            and previous_action == ActionType.RESPAWN.value
            and not in_respawn_recovery_window
        )

        running_replicas = int(app_state.get("running_replicas", 0) or 0)
        unhealthy_replicas = int(app_state.get("unhealthy_replicas", 0) or 0)
        route_prefix = app_state.get("route_prefix") or entry.get("route_prefix")

        if not ray_available:
            observed_status = previous_observed
            health_status = previous_health if previous_health != "unknown" else "degraded"
            recovery_state = previous_recovery
            failure_reason = entry.get("last_failure_reason")
            notes = "Ray collector unavailable; using durable catalog state."
        elif desired_status != "running":
            observed_status = "offline"
            health_status = "offline"
            recovery_state = "idle"
            failure_reason = None
            notes = "Deployment is intentionally not running."
        elif app_state:
            observed_status = str(app_state.get("observed_status") or "unknown")
            health_status = str(app_state.get("health_status") or "unknown")
            recovery_state = str(app_state.get("recovery_state") or "idle")
            failure_reason = app_state.get("failure_reason")
            notes = str(
                app_state.get("notes") or "Deployment state derived from Ray Serve."
            )
        elif previous_recovery == "recovering":
            observed_status = "recovering"
            health_status = "degraded"
            recovery_state = "recovering"
            failure_reason = None
            notes = "Overseer previously initiated recovery."
        else:
            observed_status = "missing"
            health_status = "offline"
            recovery_state = "idle"
            failure_reason = f"Deployment '{deployment_name}' is absent from live Serve state."
            notes = "Deployment is expected but currently missing."

        # Once Overseer starts a respawn, keep the deployment in a recovering
        # state until it either becomes ready or the recovery window expires.
        # This avoids racing transient delete/redeploy observations.
        if respawn_recovery_active and observed_status != "ready":
            observed_status = "recovering"
            health_status = "degraded"
            recovery_state = "recovering"
            failure_reason = None
            notes = (
                "Deployment is within the respawn recovery window; "
                "waiting for Ray Serve state to converge."
            )
        elif respawn_recovery_timed_out and observed_status != "ready":
            if observed_status in {"recovering", "unknown", "stale"}:
                observed_status = "missing" if not app_state else "degraded"
            if observed_status in {"missing", "offline"}:
                health_status = "offline"
            else:
                health_status = "degraded"
            recovery_state = "failed"
            failure_reason = (
                failure_reason
                or f"Respawn recovery timed out for deployment '{deployment_name}'."
            )
            notes = (
                "Overseer recovery attempt exceeded the respawn recovery timeout; "
                "the deployment is eligible for a fresh healing decision."
            )

        alive = (
            desired_status == "running"
            and observed_status in {"ready", "degraded", "recovering"}
            and health_status != "offline"
        )

        return {
            **entry,
            "name": deployment_name,
            "metadata": metadata,
            "deployment_metadata": deployment_metadata,
            "route_prefix": route_prefix,
            "managed_by_overseer": managed,
            "desired_status": desired_status,
            "observed_status": observed_status,
            "health_status": health_status,
            "recovery_state": recovery_state,
            "last_failure_reason": failure_reason,
            "reconcile_notes": notes,
            "alive": alive,
            "live_counts": {
                "alive_replicas": running_replicas,
                "dead_replicas": unhealthy_replicas,
            },
        }

    def _in_respawn_recovery_window(self, entry: dict[str, Any]) -> bool:
        if self.respawn_recovery_timeout_seconds <= 0:
            return False
        raw = entry.get("last_reconciled_at")
        if not raw:
            return False
        try:
            reconciled_at = datetime.fromisoformat(str(raw))
        except ValueError:
            return False
        if reconciled_at.tzinfo is None:
            reconciled_at = reconciled_at.replace(tzinfo=timezone.utc)
        age_seconds = (
            datetime.now(timezone.utc) - reconciled_at.astimezone(timezone.utc)
        ).total_seconds()
        return age_seconds <= self.respawn_recovery_timeout_seconds

    def _is_agent_control_healthy(
        self,
        deployments: list[dict[str, Any]],
        ray_available: bool,
    ) -> bool:
        if not ray_available:
            return False
        for deployment in deployments:
            if not deployment.get("managed_by_overseer"):
                continue
            if str(deployment.get("desired_status") or "") != "running":
                continue
            if str(deployment.get("observed_status") or "unknown") != "ready":
                return False
        return True

    def _legacy_catalog_status(self, deployment: dict[str, Any]) -> str:
        observed_status = str(deployment.get("observed_status") or "unknown")
        if observed_status in {"ready", "degraded", "recovering"}:
            return "alive"
        if observed_status == "stale":
            return "stale"
        if observed_status in {"missing", "offline"}:
            return "offline"
        return "unknown"

    def _sync_agent_catalog_state(self, deployments: list[dict[str, Any]]) -> None:
        for deployment in deployments:
            try:
                update_agent_catalog_status(
                    name=str(deployment.get("name") or ""),
                    status=self._legacy_catalog_status(deployment),
                    mark_seen=bool(deployment.get("alive")),
                    heartbeat=False,
                    observed_status=str(deployment.get("observed_status") or "unknown"),
                    health_status=str(deployment.get("health_status") or "unknown"),
                    recovery_state=str(deployment.get("recovery_state") or "idle"),
                    last_failure_reason=deployment.get("last_failure_reason"),
                    last_action_type=deployment.get("last_action_type"),
                    reconcile_notes=deployment.get("reconcile_notes"),
                    # last_reconciled_at is reserved for respawn-attempt start
                    # tracking, so normal sync must not refresh it.
                    reconciled=False,
                )
            except Exception as exc:
                logger.warning(
                    f"Failed to sync catalog state for {deployment.get('name')}: {exc}"
                )

    async def _mark_action_started(self, action: OverseerAction) -> None:
        if not action.deployment_name:
            return
        if action.type != ActionType.RESPAWN:
            return

        await asyncio.to_thread(
            update_agent_catalog_status,
            name=action.deployment_name,
            status="alive",
            mark_seen=False,
            heartbeat=False,
            observed_status="recovering",
            health_status="degraded",
            recovery_state="recovering",
            last_failure_reason=action.reason,
            last_action_type=action.type.value,
            reconcile_notes="Overseer started recovery for this deployment.",
            reconciled=True,
        )

    async def _apply_action_result(
        self,
        action: OverseerAction,
        result: ActionResult,
    ) -> None:
        if not action.deployment_name:
            return

        if result.success:
            if action.type == ActionType.RESPAWN:
                await asyncio.to_thread(
                    update_agent_catalog_status,
                    name=action.deployment_name,
                    status="alive",
                    mark_seen=False,
                    heartbeat=False,
                    observed_status="recovering",
                    health_status="degraded",
                    recovery_state="recovering",
                    last_failure_reason=None,
                    last_action_type=action.type.value,
                    reconcile_notes=result.detail or "Recovery command accepted by Ray.",
                    reconciled=False,
                )
            else:
                await asyncio.to_thread(
                    update_agent_catalog_status,
                    name=action.deployment_name,
                    status="alive",
                    mark_seen=False,
                    heartbeat=False,
                    observed_status=None,
                    health_status=None,
                    recovery_state=None,
                    last_failure_reason=None,
                    last_action_type=action.type.value,
                    reconcile_notes=result.detail or action.reason,
                    reconciled=False,
                )
            return

        await asyncio.to_thread(
            update_agent_catalog_status,
            name=action.deployment_name,
            status="offline",
            mark_seen=False,
            heartbeat=False,
            observed_status="missing",
            health_status="degraded",
            recovery_state="failed",
            last_failure_reason=result.error or result.detail or action.reason,
            last_action_type=action.type.value,
            reconcile_notes="Overseer action failed; manual inspection may be required.",
            reconciled=False,
        )

    def _cooldown_scope(self, action: OverseerAction) -> str:
        if action.deployment_name:
            return action.deployment_name
        if action.agent_class:
            return action.agent_class
        if action.agent:
            return action.agent
        return action.target

    def _build_action_alert(
        self,
        action: OverseerAction,
        result: ActionResult,
    ) -> dict[str, Any]:
        payload = action.to_alert()
        payload["status"] = "succeeded" if result.success else "failed"
        payload["detail"] = result.detail or result.error or action.reason
        payload["result_detail"] = result.detail
        payload["result_error"] = result.error
        payload["target"] = action.deployment_name or action.target
        if not result.success:
            payload["level"] = "error"
        elif action.type == ActionType.RESPAWN:
            payload["level"] = "warning"
        elif action.type == ActionType.ALERT:
            payload["level"] = "warning"
        return payload

    async def _safe_collect(self, collector: BaseCollector) -> ServiceMetrics:
        """Run a single collector with error handling."""
        try:
            return await collector.collect()
        except Exception as exc:
            return ServiceMetrics(
                service=collector.endpoint.name,
                healthy=False,
                error=str(exc),
            )
