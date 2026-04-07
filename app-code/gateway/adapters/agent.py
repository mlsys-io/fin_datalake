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
import logging
from datetime import datetime, timezone
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
        kwargs = {}
        if session_id is not None:
            kwargs["session_id"] = session_id

        response = await self._call_agent_method(agent_name, method_name, payload, **kwargs)
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
            await self._call_agent_method(agent_name, "handle_event", event)
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
        from gateway.core.ray_client import list_agents_from_hub

        return await asyncio.to_thread(list_agents_from_hub)

    async def _call_agent_method(self, agent_name: str, method_name: str, *args: Any, **kwargs: Any) -> Any:
        from gateway.core.ray_client import call_agent_method

        return await asyncio.to_thread(call_agent_method, agent_name, method_name, *args, **kwargs)

    async def _get_catalog_agents(self, live_agents: list[dict]) -> list[dict]:
        live_by_name = {}
        runtime_namespace = os.getenv("RAY_NAMESPACE") or "serve"
        for agent in live_agents:
            normalized_agent = self._normalize_live_agent(agent, runtime_namespace=runtime_namespace)
            agent_name = str(normalized_agent.get("name") or "").strip()
            if agent_name:
                live_by_name[agent_name] = normalized_agent

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
                merged.append(self._merge_catalog_and_live_agent(catalog_agent, live_agent))
            else:
                merged.append({
                    **catalog_agent,
                    "alive": catalog_agent.get("alive", False),
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

    def _normalize_live_agent(self, agent: dict, *, runtime_namespace: str) -> dict:
        agent_name = str(agent.get("name") or "").strip()
        route_prefix = agent.get("route_prefix") or (f"/{agent_name}" if agent_name else None)
        is_alive = bool(agent.get("alive", False))

        normalized = dict(agent)
        normalized.setdefault("runtime_source", "ray-serve")
        normalized.setdefault("runtime_namespace", runtime_namespace)
        normalized.setdefault("route_prefix", route_prefix)
        normalized.setdefault("status", "alive" if is_alive else "unknown")
        normalized.setdefault("managed_by_overseer", True)
        normalized.setdefault("desired_status", "running")
        normalized.setdefault("observed_status", "ready" if is_alive else "unknown")
        normalized.setdefault("health_status", "healthy" if is_alive else "unknown")
        normalized.setdefault("recovery_state", "idle")
        normalized.setdefault("last_failure_reason", None)
        normalized.setdefault("last_action_type", None)
        normalized.setdefault("reconcile_notes", None)
        normalized.setdefault("deployment_metadata", {})

        now = datetime.now(timezone.utc).isoformat()
        normalized.setdefault("last_seen_at", now)
        normalized.setdefault("last_heartbeat_at", now)
        normalized.setdefault("last_reconciled_at", now if is_alive else None)
        return normalized

    def _merge_catalog_and_live_agent(self, catalog_agent: dict, live_agent: dict) -> dict:
        merged = dict(catalog_agent)

        passthrough_fields = (
            "capabilities",
            "capability_specs",
            "metadata",
            "deployment_metadata",
            "runtime_source",
            "runtime_namespace",
            "route_prefix",
            "registered_at",
            "last_seen_at",
            "last_heartbeat_at",
        )
        for field in passthrough_fields:
            if live_agent.get(field) is not None:
                merged[field] = live_agent[field]

        merged["source"] = "catalog+runtime"

        # Preserve the richer control-plane status coming from the catalog.
        if not merged.get("status"):
            merged["status"] = live_agent.get("status", "unknown")
        merged["alive"] = (
            str(merged.get("desired_status") or "") == "running"
            and str(merged.get("observed_status") or "unknown") in {"ready", "degraded", "recovering"}
            and str(merged.get("health_status") or "unknown") != "offline"
        )
        return merged
