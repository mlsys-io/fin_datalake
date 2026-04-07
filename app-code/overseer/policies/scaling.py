"""
KafkaLagPolicy — Scale agents up/down based on consumer group lag.

If total lag exceeds a threshold, emit ScaleUp actions.
If lag is zero and agents are idle, emit ScaleDown actions.
"""

from __future__ import annotations

from overseer.models import ActionType, OverseerAction, SystemSnapshot
from overseer.policies.base import BasePolicy
from overseer.agent_registry import get_default_agent_name


class ResourceExhaustionPolicy(BasePolicy):
    """
    Analyzes the Ray cluster CPU usage to detect generic overload.
    Scales up if CPU > threshold, scales down if CPU < idle_threshold.
    """

    def __init__(
        self,
        scale_up_cpu_threshold: float = 85.0,
        scale_down_cpu_threshold: float = 20.0,
        agent_class: str | None = None,
        scale_up_count: int = 1,
        scale_down_count: int = 1,
    ):
        self.scale_up_cpu_threshold = scale_up_cpu_threshold
        self.scale_down_cpu_threshold = scale_down_cpu_threshold
        self.agent_class = agent_class or get_default_agent_name()
        self.scale_up_count = scale_up_count
        self.scale_down_count = scale_down_count

    def evaluate(self, snapshot: SystemSnapshot) -> list[OverseerAction]:
        actions: list[OverseerAction] = []
        ray_metrics = snapshot.services.get("ray")
        if not ray_metrics or not ray_metrics.healthy:
            return actions

        cluster_status = ray_metrics.data.get("cluster", {})
        if not cluster_status:
            return actions

        data = cluster_status.get("data", {})
        status = data.get("clusterStatus", {})
        
        try:
            load = status.get("loadMetrics", {})
            total = status.get("totalResources", {})
            
            cpu_used = float(load.get("CPU", 0.0))
            cpu_total = float(total.get("CPU", 1.0))
            cpu_percent = (cpu_used / cpu_total) * 100.0
        except (ValueError, TypeError):
            import loguru
            loguru.logger.debug("ResourceExhaustionPolicy could not parse CPU metrics from cluster_status.")
            return actions

        alive_agents = ray_metrics.data.get("actors_alive", 0)

        if cpu_percent > self.scale_up_cpu_threshold:
            actions.append(OverseerAction(
                type=ActionType.SCALE_UP,
                target="ray",
                agent=self.agent_class,
                count=self.scale_up_count,
                reason=f"Cluster CPU is at {cpu_percent:.1f}% (Threshold: {self.scale_up_cpu_threshold}%). Overload detected, scaling up.",
            ))

        elif cpu_percent < self.scale_down_cpu_threshold and alive_agents > 1:
            actions.append(OverseerAction(
                type=ActionType.SCALE_DOWN,
                target="ray",
                agent=self.agent_class,
                count=self.scale_down_count,
                reason=f"Cluster CPU is low at {cpu_percent:.1f}% and {alive_agents} agents are alive. Scaling down.",
            ))

        return actions


class KafkaLagPolicy(BasePolicy):

    def __init__(
        self,
        scale_up_threshold: int = 500,
        scale_down_idle_threshold: int = 0,
        agent_class: str | None = None,
        scale_up_count: int = 2,
        scale_down_count: int = 1,
    ):
        self.scale_up_threshold = scale_up_threshold
        self.scale_down_idle_threshold = scale_down_idle_threshold
        self.agent_class = agent_class or get_default_agent_name()
        self.scale_up_count = scale_up_count
        self.scale_down_count = scale_down_count

    def evaluate(self, snapshot: SystemSnapshot) -> list[OverseerAction]:
        actions: list[OverseerAction] = []
        kafka = snapshot.services.get("kafka")
        if not kafka or not kafka.healthy:
            return actions

        total_lag = kafka.data.get("total_lag", 0)
        lag_by_topic = kafka.data.get("lag_by_topic", {})

        # Scale up: lag is too high
        if total_lag > self.scale_up_threshold:
            worst_topic = max(lag_by_topic, key=lag_by_topic.get) if lag_by_topic else "unknown"
            actions.append(OverseerAction(
                type=ActionType.SCALE_UP,
                target="ray",
                agent=self.agent_class,
                count=self.scale_up_count,
                reason=f"Kafka lag on '{worst_topic}' is {lag_by_topic.get(worst_topic, total_lag)} "
                       f"(total: {total_lag}, threshold: {self.scale_up_threshold})",
            ))

        # Scale down: lag is zero — could release idle agents
        elif total_lag <= self.scale_down_idle_threshold:
            ray_metrics = snapshot.services.get("ray")
            if ray_metrics and ray_metrics.healthy:
                alive = ray_metrics.data.get("actors_alive", 0)
                if alive > 2:  # Keep at least 2 agents alive
                    actions.append(OverseerAction(
                        type=ActionType.SCALE_DOWN,
                        target="ray",
                        agent=self.agent_class,
                        count=self.scale_down_count,
                        reason=f"Kafka lag is {total_lag} and {alive} agents are alive. Scaling down.",
                    ))

        return actions
