"""
KafkaLagPolicy — Scale agents up/down based on consumer group lag.

If total lag exceeds a threshold, emit ScaleUp actions.
If lag is zero and agents are idle, emit ScaleDown actions.
"""

from __future__ import annotations

from overseer.models import ActionType, OverseerAction, SystemSnapshot
from overseer.policies.base import BasePolicy
from overseer.agent_registry import get_default_agent_name


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
