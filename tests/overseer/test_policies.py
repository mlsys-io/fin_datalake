"""
Tests for the Overseer policy engine: KafkaLagPolicy and ActorHealthPolicy.
"""

import pytest
from overseer.models import (
    ActionType,
    ServiceMetrics,
    SystemSnapshot,
)
from overseer.policies.scaling import KafkaLagPolicy
from overseer.policies.healing import ActorHealthPolicy


# ---------------------------------------------------------------------------
# Helper to build snapshots
# ---------------------------------------------------------------------------

def make_snapshot(kafka_lag: dict | None = None, actors: list | None = None) -> SystemSnapshot:
    """Build a minimal SystemSnapshot for policy testing."""
    snap = SystemSnapshot()
    if kafka_lag is not None:
        snap.services["kafka"] = ServiceMetrics(
            service="kafka", healthy=True,
            data={"lag_by_topic": kafka_lag, "total_lag": sum(kafka_lag.values())},
        )
    if actors is not None:
        alive = sum(1 for a in actors if a["state"] == "ALIVE")
        dead = sum(1 for a in actors if a["state"] == "DEAD")
        snap.services["ray"] = ServiceMetrics(
            service="ray", healthy=True,
            data={"actors": actors, "actors_alive": alive, "actors_dead": dead},
        )
    return snap


# ---------------------------------------------------------------------------
# KafkaLagPolicy
# ---------------------------------------------------------------------------

class TestKafkaLagPolicy:
    def setup_method(self):
        self.policy = KafkaLagPolicy(
            scale_up_threshold=500,
            scale_up_count=3,
            agent_class="SentimentAgent",
        )

    def test_no_action_when_lag_below_threshold(self):
        snap = make_snapshot(kafka_lag={"news": 100})
        actions = self.policy.evaluate(snap)
        assert len(actions) == 0

    def test_scale_up_when_lag_exceeds_threshold(self):
        snap = make_snapshot(kafka_lag={"news": 1000})
        actions = self.policy.evaluate(snap)
        assert len(actions) == 1
        assert actions[0].type == ActionType.SCALE_UP
        assert actions[0].agent == "SentimentAgent"
        assert actions[0].count == 3

    def test_scale_up_identifies_worst_topic(self):
        snap = make_snapshot(kafka_lag={"news": 200, "market_data": 800})
        actions = self.policy.evaluate(snap)
        assert "market_data" in actions[0].reason

    def test_no_action_when_kafka_service_missing(self):
        snap = SystemSnapshot()  # No kafka metrics at all
        actions = self.policy.evaluate(snap)
        assert len(actions) == 0

    def test_no_action_when_kafka_unhealthy(self):
        snap = SystemSnapshot()
        snap.services["kafka"] = ServiceMetrics(service="kafka", healthy=False)
        actions = self.policy.evaluate(snap)
        assert len(actions) == 0

    def test_scale_down_when_lag_zero_and_agents_alive(self):
        snap = make_snapshot(
            kafka_lag={"news": 0},
            actors=[
                {"actor_id": "1", "class_name": "SentimentAgent", "state": "ALIVE"},
                {"actor_id": "2", "class_name": "SentimentAgent", "state": "ALIVE"},
                {"actor_id": "3", "class_name": "SentimentAgent", "state": "ALIVE"},
            ],
        )
        policy = KafkaLagPolicy(scale_down_idle_threshold=0, scale_down_count=1)
        actions = policy.evaluate(snap)
        assert any(a.type == ActionType.SCALE_DOWN for a in actions)


# ---------------------------------------------------------------------------
# ActorHealthPolicy
# ---------------------------------------------------------------------------

class TestActorHealthPolicy:
    def setup_method(self):
        self.policy = ActorHealthPolicy()

    def test_no_action_when_all_alive(self):
        snap = make_snapshot(actors=[
            {"actor_id": "1", "class_name": "SentimentAgent", "state": "ALIVE"},
            {"actor_id": "2", "class_name": "TraderAgent", "state": "ALIVE"},
        ])
        actions = self.policy.evaluate(snap)
        assert len(actions) == 0

    def test_respawn_for_dead_managed_agent(self):
        snap = make_snapshot(actors=[
            {"actor_id": "1", "class_name": "SentimentAgent", "state": "DEAD"},
        ])
        actions = self.policy.evaluate(snap)
        assert len(actions) == 1
        assert actions[0].type == ActionType.RESPAWN
        assert actions[0].agent == "SentimentAgent"

    def test_ignores_dead_unmanaged_actors(self):
        """Actors not in MANAGED_AGENTS should be ignored."""
        snap = make_snapshot(actors=[
            {"actor_id": "1", "class_name": "RandomUnknownActor", "state": "DEAD"},
        ])
        actions = self.policy.evaluate(snap)
        assert len(actions) == 0

    def test_multiple_dead_actors_produce_multiple_actions(self):
        snap = make_snapshot(actors=[
            {"actor_id": "1", "class_name": "SentimentAgent", "state": "DEAD"},
            {"actor_id": "2", "class_name": "TraderAgent", "state": "DEAD"},
            {"actor_id": "3", "class_name": "MarketAnalyst", "state": "ALIVE"},
        ])
        actions = self.policy.evaluate(snap)
        assert len(actions) == 2

    def test_no_action_when_ray_missing(self):
        snap = SystemSnapshot()
        actions = self.policy.evaluate(snap)
        assert len(actions) == 0
