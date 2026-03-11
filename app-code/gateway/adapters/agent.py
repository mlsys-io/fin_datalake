"""
AgentAdapter: Domain "agent"

Handles the Intelligence Plane: interacting with Ray-based Agent Actors.

Supported Actions:
    - chat:      Synchronous request-response to a named Agent (via AgentHub).
    - notify:    Fire-and-forget notification to all agents.
    - list:      List all registered agents and their capabilities.

Required Permissions:
    - chat / list:  agent:interact / agent:read
    - notify:       agent:broadcast (system-wide impact)
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
            "notify": self._notify,
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
        """Synchronous ask/response to a named Agent via AgentHub."""
        self._require_permission(user, Permission.AGENT_INTERACT)
        import ray
        from etl.agents.hub import get_hub

        agent_name = intent.parameters.get("agent_name")
        message = intent.parameters.get("message")
        if not agent_name or message is None:
            raise ValueError("Parameters 'agent_name' and 'message' are required.")

        hub = get_hub()
        response = ray.get(hub.call.remote(
            name=agent_name,
            payload={"sender": user.username, "payload": message},
        ))
        return {"agent": agent_name, "response": response}

    def _notify(self, user: User, intent: UserIntent) -> dict:
        """Broadcast a notification to all agents via AgentHub."""
        self._require_permission(user, Permission.AGENT_BROADCAST)
        import ray
        from etl.agents.hub import get_hub

        payload = intent.parameters.get("payload")
        event = {
            "topic": "_broadcast",
            "payload": payload,
            "sender": user.username,
        }
        hub = get_hub()
        delivered = ray.get(hub.notify_all.remote(event))
        return {"delivered_to": delivered}

    def _list_agents(self, user: User, intent: UserIntent) -> dict:
        """List all registered agents via AgentHub."""
        self._require_permission(user, Permission.AGENT_READ)
        import ray
        from etl.agents.hub import get_hub

        hub = get_hub()
        agents = ray.get(hub.list_agents.remote())
        return {"agents": agents}
