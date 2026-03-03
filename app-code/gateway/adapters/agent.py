"""
AgentAdapter: Domain "agent"

Handles the Intelligence Plane: interacting with Ray-based Agent Actors.

Supported Actions:
    - chat:      Synchronous request-response to a named Agent.
    - broadcast: Publish a message to all subscribers on a MessageBus topic.
    - list:      List all registered agents and their capabilities.

Required Permissions:
    - chat / list:  agent:interact / agent:read
    - broadcast:    agent:broadcast (system-wide impact)
"""

from typing import Any

from gateway.core.adapters import BaseAdapter, ActionNotFoundError
from gateway.models.intent import UserIntent
from gateway.models.user import Permission, User


class AgentAdapter(BaseAdapter):

    def handles(self) -> str:
        return "agent"

    def execute(self, user: User, intent: UserIntent) -> Any:
        dispatch = {
            "chat": self._chat,
            "broadcast": self._broadcast,
            "list": self._list_agents,
        }
        handler = dispatch.get(intent.action)
        if handler is None:
            raise ActionNotFoundError(
                f"AgentAdapter does not support action '{intent.action}'. "
                f"Available: {list(dispatch.keys())}"
            )
        return handler(user, intent)

    def _chat(self, user: User, intent: UserIntent) -> dict:
        """Synchronous ask/response to a named Agent. Requires agent:interact."""
        self._require_permission(user, Permission.AGENT_INTERACT)
        import ray
        from etl.agents.registry import get_registry
        agent_name = intent.parameters.get("agent_name")
        message = intent.parameters.get("message")
        if not agent_name or message is None:
            raise ValueError("Parameters 'agent_name' and 'message' are required.")
        registry = get_registry()
        actor_handle = ray.get(registry.get_agent.remote(agent_name))
        if actor_handle is None:
            raise ValueError(f"Agent '{agent_name}' is not registered or not alive.")
        response = ray.get(actor_handle.ask.remote({"sender": user.username, "payload": message}))
        return {"agent": agent_name, "response": response}

    def _broadcast(self, user: User, intent: UserIntent) -> dict:
        """Publish to the MessageBus. Requires agent:broadcast."""
        self._require_permission(user, Permission.AGENT_BROADCAST)
        import ray
        from etl.agents.bus import get_bus
        topic = intent.parameters.get("topic")
        payload = intent.parameters.get("payload")
        if not topic:
            raise ValueError("Parameter 'topic' is required.")
        bus = get_bus()
        delivered = ray.get(bus.publish.remote(topic=topic, payload=payload, sender=user.username))
        return {"topic": topic, "delivered_to": delivered}

    def _list_agents(self, user: User, intent: UserIntent) -> dict:
        """List all registered agents. Requires agent:read."""
        self._require_permission(user, Permission.AGENT_READ)
        import ray
        from etl.agents.registry import get_registry
        registry = get_registry()
        agents = ray.get(registry.list_agents.remote())
        return {"agents": agents}
