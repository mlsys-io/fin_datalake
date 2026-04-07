"""
ActorHealthPolicy - deployment-aware healing for Serve-backed agents.

The policy now consumes the normalized agent_control view produced by the
Overseer loop. That keeps healing logic aligned with the durable catalog and
the same richer states shown in the dashboard.
"""

from __future__ import annotations

from overseer.agent_registry import get_managed_agent_names
from overseer.models import ActionType, OverseerAction, SystemSnapshot
from overseer.policies.base import BasePolicy

DEFAULT_MANAGED_AGENTS = {
    "SentimentAgent",
    "MarketAnalystAgent",
    "StrategyAgent",
    "TraderAgent",
    "MarketAnalyst",
    "Coordinator",
}


class ActorHealthPolicy(BasePolicy):
    def evaluate(self, snapshot: SystemSnapshot) -> list[OverseerAction]:
        actions: list[OverseerAction] = []
        control_metrics = snapshot.services.get("agent_control")
        if not control_metrics:
            return actions

        managed_agents = set(DEFAULT_MANAGED_AGENTS)
        managed_agents.update(get_managed_agent_names())

        deployments = control_metrics.data.get("deployments", [])
        for deployment in deployments:
            if not deployment.get("managed_by_overseer"):
                continue
            if str(deployment.get("desired_status") or "") != "running":
                continue

            metadata = deployment.get("metadata") or {}
            deployment_metadata = deployment.get("deployment_metadata") or {}
            class_name = str(
                metadata.get("class")
                or deployment.get("agent_class")
                or ""
            ).strip()
            if not class_name or class_name not in managed_agents:
                continue

            replication_mode = str(
                deployment_metadata.get("replication_mode") or "serve"
            ).strip().lower()
            if replication_mode != "serve":
                continue

            deployment_name = str(
                deployment.get("name")
                or metadata.get("app_name")
                or ""
            ).strip()
            if not deployment_name:
                continue

            observed_status = str(deployment.get("observed_status") or "unknown")
            health_status = str(deployment.get("health_status") or "unknown")
            recovery_state = str(deployment.get("recovery_state") or "idle")

            if recovery_state == "recovering":
                continue

            if observed_status in {"missing", "offline"}:
                actions.append(
                    OverseerAction(
                        type=ActionType.RESPAWN,
                        target="ray",
                        agent=class_name,
                        agent_class=class_name,
                        deployment_name=deployment_name,
                        runtime_namespace=str(
                            deployment.get("runtime_namespace")
                            or metadata.get("runtime_namespace")
                            or ""
                        ),
                        route_prefix=str(
                            deployment.get("route_prefix")
                            or metadata.get("route_prefix")
                            or f"/{deployment_name}"
                        ),
                        deployment_metadata=deployment_metadata,
                        count=1,
                        reason=(
                            deployment.get("last_failure_reason")
                            or f"Deployment '{deployment_name}' is {observed_status}; restoring same identity."
                        ),
                    )
                )
                continue

            if observed_status == "degraded" or health_status == "degraded":
                actions.append(
                    OverseerAction(
                        type=ActionType.ALERT,
                        target="alert",
                        agent=class_name,
                        agent_class=class_name,
                        deployment_name=deployment_name,
                        runtime_namespace=str(
                            deployment.get("runtime_namespace")
                            or metadata.get("runtime_namespace")
                            or ""
                        ),
                        route_prefix=str(
                            deployment.get("route_prefix")
                            or metadata.get("route_prefix")
                            or f"/{deployment_name}"
                        ),
                        deployment_metadata=deployment_metadata,
                        reason=(
                            deployment.get("last_failure_reason")
                            or f"Deployment '{deployment_name}' is degraded and should be inspected."
                        ),
                    )
                )

        return actions
