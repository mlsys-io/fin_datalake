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
import asyncio
import inspect
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

        return await self._invoke_handle(user, agent_name=agent_name, method_name="chat", payload=message, session_id=session_id)

    async def _invoke(self, user: User, intent: UserIntent) -> dict:
        """Generic invoke against a named agent with arbitrary payload."""
        self._require_permission(user, Permission.AGENT_INTERACT)

        agent_name = intent.parameters.get("agent_name")
        payload = intent.parameters.get("payload")
        session_id = intent.parameters.get("session_id")

        if not agent_name or payload is None:
            raise ValueError("Parameters 'agent_name' and 'payload' are required.")

        return await self._invoke_handle(user, agent_name=agent_name, method_name="invoke", payload=payload, session_id=session_id)

    async def _invoke_handle(self, user: User, agent_name: str, method_name: str, payload: Any, session_id: str | None = None) -> dict:
        handle = await self._get_agent_handle(agent_name)
        method = getattr(handle, method_name)

        kwargs = {}
        if session_id is not None:
            kwargs["session_id"] = session_id

        response = await self._call_handle_method(method, payload, **kwargs)
        return {"agent": agent_name, "response": response}

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

        tasks = [self._notify_one(name, event) for name in agent_names]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        delivered = sum(1 for r in results if r is True)
        return {"delivered_to": delivered, "total_targets": len(agent_names)}

    async def _notify_one(self, agent_name: str, event: dict) -> bool:
        try:
            handle = await self._get_agent_handle(agent_name)
            method = getattr(handle, "handle_event")
            await self._call_handle_method(method, event)
            return True
        except Exception as e:
            logger.warning("Failed to notify agent '%s': %s", agent_name, e, exc_info=True)
            return False

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
        from etl.runtime import ensure_ray

        namespace = os.getenv("RAY_NAMESPACE")
        init_kwargs = {}
        if namespace:
            init_kwargs["namespace"] = namespace
        ensure_ray(address=os.getenv("RAY_ADDRESS", "auto"), **init_kwargs)

        hub = get_hub()
        return await asyncio.to_thread(ray.get, hub.list_agents.remote())

    async def _get_agent_handle(self, agent_name: str):
        from etl.runtime import ensure_ray
        import ray.serve as serve

        ensure_ray(address=os.getenv("RAY_ADDRESS", "auto"))
        return serve.get_app_handle(agent_name)

    async def _call_handle_method(self, method: Any, *args: Any, **kwargs: Any) -> Any:
        from etl.runtime import resolve_serve_response

        remote = getattr(method, "remote", None)
        if callable(remote):
            return await asyncio.to_thread(resolve_serve_response, remote(*args, **kwargs))

        result = method(*args, **kwargs)
        if inspect.isawaitable(result):
            return await result
        return result

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
