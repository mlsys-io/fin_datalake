"""
AgentAdapter: Domain "agent"

Handles the Intelligence Plane: interacting with Ray-based Agent Actors.

Supported Actions:
    - chat:      Synchronous request-response to a named chat-capable Agent.
    - invoke:    Generic invoke-response to a named agent with arbitrary payload.
    - notify:    Fire-and-forget notification to all agents.
    - list:      List all registered agents and their capabilities.

Required Permissions:
    - chat / list:  agent:interact / agent:read
    - notify:       agent:broadcast (system-wide impact)
"""

import os
import httpx
import asyncio
import logging
from typing import Any

from gateway.core.adapters import BaseAdapter, ActionNotFoundError
from gateway.core.rbac import Permission
from gateway.db import crud
from gateway.db.session import AsyncSessionLocal
from gateway.models.intent import UserIntent
from gateway.models.user import User

logger = logging.getLogger(__name__)


class AgentAdapter(BaseAdapter):

    def handles(self) -> str:
        return "agent"

    async def execute(self, user: User, intent: UserIntent) -> Any:
        dispatch = {
            "chat": self._chat,
            "invoke": self._invoke,
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
        """Synchronous chat-style interaction with a named agent."""
        self._require_permission(user, Permission.AGENT_INTERACT)

        agent_name = intent.parameters.get("agent_name")
        message = intent.parameters.get("message")
        session_id = intent.parameters.get("session_id")

        if not agent_name or message is None:
            raise ValueError("Parameters 'agent_name' and 'message' are required.")

        return await self._invoke_http(user, agent_name=agent_name, payload=message, session_id=session_id)

    async def _invoke(self, user: User, intent: UserIntent) -> dict:
        """Generic invoke against a named agent with arbitrary payload."""
        self._require_permission(user, Permission.AGENT_INTERACT)

        agent_name = intent.parameters.get("agent_name")
        payload = intent.parameters.get("payload")
        session_id = intent.parameters.get("session_id")

        if not agent_name or payload is None:
            raise ValueError("Parameters 'agent_name' and 'payload' are required.")

        return await self._invoke_http(user, agent_name=agent_name, payload=payload, session_id=session_id)

    async def _invoke_http(self, user: User, agent_name: str, payload: Any, session_id: str | None = None) -> dict:
        endpoint = os.getenv("RAY_SERVE_ENDPOINT", "http://localhost:8000")
        url = f"{endpoint}/{agent_name}/invoke"

        async with httpx.AsyncClient(timeout=60.0) as client:
            request_body = {
                "payload": payload,
                "session_id": session_id,
                "metadata": {"sender": user.username}
            }
            response = await client.post(url, json=request_body)
            response.raise_for_status()
            return {"agent": agent_name, "response": response.json()}

    async def _notify(self, user: User, intent: UserIntent) -> dict:
        """Broadcast an event to all agents via HTTP."""
        self._require_permission(user, Permission.AGENT_BROADCAST)

        agents_info = await self._fetch_agents_from_hub()
        agent_names = [a["name"] for a in agents_info if a.get("alive", False)]

        payload = intent.parameters.get("payload")
        event = {
            "type": "_broadcast",
            "payload": payload,
            "sender": user.username,
        }

        endpoint = os.getenv("RAY_SERVE_ENDPOINT", "http://localhost:8000")

        async with httpx.AsyncClient(timeout=10.0) as client:
            tasks = [
                client.post(f"{endpoint}/{name}/events", json=event)
                for name in agent_names
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        delivered = sum(1 for r in results if isinstance(r, httpx.Response) and r.status_code == 200)
        return {"delivered_to": delivered, "total_targets": len(agent_names)}

    async def _list_agents(self, user: User, intent: UserIntent) -> dict:
        """List agents from the durable catalog, enriched with live AgentHub state when available."""
        self._require_permission(user, Permission.AGENT_READ)

        live_agents: list[dict] = []
        live_available = False
        detail = None

        try:
            live_agents = await self._fetch_agents_from_hub()
            live_available = True
        except Exception as e:
            logger.warning("AgentHub listing unavailable: %s", e, exc_info=True)
            detail = f"AgentHub unavailable: {e}"

        agents = await self._get_catalog_agents(live_agents)
        return {
            "agents": agents,
            "available": live_available,
            "detail": detail,
        }

    async def _fetch_agents_from_hub(self) -> list[dict]:
        import ray
        from etl.agents.hub import get_hub

        if not ray.is_initialized():
            init_kwargs = {
                "address": os.getenv("RAY_ADDRESS", "auto"),
                "ignore_reinit_error": True,
            }
            namespace = os.getenv("RAY_NAMESPACE")
            if namespace:
                init_kwargs["namespace"] = namespace
            ray.init(**init_kwargs)

        hub = get_hub()
        return await asyncio.to_thread(ray.get, hub.list_agents.remote())

    async def _get_catalog_agents(self, live_agents: list[dict]) -> list[dict]:
        live_by_name = {}
        for agent in live_agents:
            agent_name = str(agent.get("name") or "").strip()
            if agent_name:
                live_by_name[agent_name] = agent

        async with AsyncSessionLocal() as db:
            for agent in live_by_name.values():
                try:
                    await crud.upsert_agent_definition(db, agent)
                except Exception as e:
                    logger.warning("Failed to sync agent '%s' into catalog: %s", agent.get("name"), e, exc_info=True)

            catalog_agents = await crud.list_agent_definitions(db)

        merged = []
        seen = set()

        for catalog_agent in catalog_agents:
            name = catalog_agent["name"]
            live_agent = live_by_name.get(name)
            if live_agent:
                merged.append({
                    **catalog_agent,
                    **live_agent,
                    "source": "catalog+runtime",
                })
            else:
                merged.append({
                    **catalog_agent,
                    "alive": False,
                    "source": catalog_agent.get("source", "catalog"),
                })
            seen.add(name)

        for name, live_agent in live_by_name.items():
            if name in seen:
                continue
            merged.append({
                **live_agent,
                "source": "runtime",
            })

        merged.sort(key=lambda agent: str(agent.get("name") or "").lower())
        return merged
