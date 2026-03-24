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

import os
import httpx
import asyncio
from typing import Any

from gateway.core.adapters import BaseAdapter, ActionNotFoundError
from gateway.core.rbac import Permission
from gateway.models.intent import UserIntent
from gateway.models.user import User


class AgentAdapter(BaseAdapter):

    def handles(self) -> str:
        return "agent"

    async def execute(self, user: User, intent: UserIntent) -> Any:
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
        
        return await handler(user, intent)


    async def _chat(self, user: User, intent: UserIntent) -> dict:
        """Synchronous ask/response to a Ray Serve Agent via HTTP."""
        self._require_permission(user, Permission.AGENT_INTERACT)
        
        agent_name = intent.parameters.get("agent_name")
        message = intent.parameters.get("message")
        session_id = intent.parameters.get("session_id")
        
        if not agent_name or message is None:
            raise ValueError("Parameters 'agent_name' and 'message' are required.")

        endpoint = os.getenv("RAY_SERVE_ENDPOINT", "http://localhost:8000")
        url = f"{endpoint}/{agent_name}/ask"
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            payload = {
                "payload": message,
                "session_id": session_id,
                "metadata": {"sender": user.username}
            }
            response = await client.post(url, json=payload)

            response.raise_for_status()
            return {"agent": agent_name, "response": response.json()}

    async def _notify(self, user: User, intent: UserIntent) -> dict:
        """Broadcast a notification to all agents via HTTP."""
        self._require_permission(user, Permission.AGENT_BROADCAST)
        
        # 1. Get the list of agents from Hub
        import ray
        from etl.agents.hub import get_hub
        hub = get_hub()
        agents_info = await hub.list_agents.remote()
        agent_names = [a["name"] for a in agents_info if a.get("alive", False)]

        # 2. Fan-out HTTP requests
        payload = intent.parameters.get("payload")
        event = {
            "topic": "_broadcast",
            "payload": payload,
            "sender": user.username,
        }
        
        endpoint = os.getenv("RAY_SERVE_ENDPOINT", "http://localhost:8000")
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            tasks = [
                client.post(f"{endpoint}/{name}/notify", json=event)
                for name in agent_names
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
        delivered = sum(1 for r in results if isinstance(r, httpx.Response) and r.status_code == 200)
        return {"delivered_to": delivered, "total_targets": len(agent_names)}

    async def _list_agents(self, user: User, intent: UserIntent) -> dict:
        """List all registered agents via AgentHub."""
        self._require_permission(user, Permission.AGENT_READ)
        import ray
        from etl.agents.hub import get_hub

        hub = get_hub()
        agents = await hub.list_agents.remote()
        return {"agents": agents}
