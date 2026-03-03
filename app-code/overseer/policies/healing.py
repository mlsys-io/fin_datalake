"""
ActorHealthPolicy — Detect and respawn dead Ray actors.

Scans the actor list from RayCollector and emits Respawn actions
for any managed agent class found in a DEAD state.
"""

from __future__ import annotations

from overseer.models import ActionType, OverseerAction, SystemSnapshot
from overseer.policies.base import BasePolicy

# Agent classes the Overseer is responsible for managing.
# Add new agent types here as they are created.
MANAGED_AGENTS = {"SentimentAgent", "TraderAgent", "MarketAnalyst", "Coordinator"}


class ActorHealthPolicy(BasePolicy):

    def evaluate(self, snapshot: SystemSnapshot) -> list[OverseerAction]:
        actions: list[OverseerAction] = []
        ray_metrics = snapshot.services.get("ray")
        if not ray_metrics or not ray_metrics.healthy:
            return actions

        for actor in ray_metrics.data.get("actors", []):
            class_name = actor.get("class_name", "")
            state = actor.get("state", "")
            if class_name in MANAGED_AGENTS and state == "DEAD":
                actions.append(OverseerAction(
                    type=ActionType.RESPAWN,
                    target="ray",
                    agent=class_name,
                    count=1,
                    reason=f"Actor '{class_name}' (id: {actor.get('actor_id', '?')}) found DEAD. Respawning.",
                ))

        return actions
