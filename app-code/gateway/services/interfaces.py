from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from gateway.core.registry import build_default_registry
from gateway.core.rbac import Permission
from gateway.mcp.tool_registry import GatewayMcpToolRegistry
from gateway.mcp.tools import agent as agent_tools
from gateway.mcp.tools import broker as broker_tools
from gateway.mcp.tools import compute as compute_tools
from gateway.mcp.tools import data as data_tools
from gateway.mcp.tools import system as system_tools
from gateway.services.system import probe_infra_targets


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


_INTERFACE_ROUTES: list[dict[str, str]] = [
    {"method": "POST", "path": "/api/v1/intent", "domain": "*", "action": "dispatch"},
    {"method": "GET", "path": "/api/v1/intent/domains", "domain": "*", "action": "discovery"},
    {"method": "GET", "path": "/api/v1/agents", "domain": "agent", "action": "list"},
    {"method": "POST", "path": "/api/v1/agents/{agent_name}/chat", "domain": "agent", "action": "chat"},
    {"method": "POST", "path": "/api/v1/agents/{agent_name}/invoke", "domain": "agent", "action": "invoke"},
    {"method": "POST", "path": "/api/v1/agents/broadcast", "domain": "agent", "action": "notify"},
    {"method": "GET", "path": "/api/v1/system/overseer/snapshots", "domain": "system", "action": "overseer_snapshots"},
    {"method": "GET", "path": "/api/v1/system/overseer/alerts", "domain": "system", "action": "overseer_alerts"},
    {"method": "GET", "path": "/api/v1/system/infra/status", "domain": "system", "action": "infra_status"},
    {"method": "GET", "path": "/api/v1/system/audit-logs", "domain": "system", "action": "audit_logs"},
    {"method": "GET", "path": "/api/v1/system/interfaces", "domain": "system", "action": "interface_inventory"},
]


def _collect_mcp_tools() -> list[dict[str, Any]]:
    registry = GatewayMcpToolRegistry()
    for module in (data_tools, compute_tools, agent_tools, system_tools, broker_tools):
        registry.register_many(module.get_tool_specs())
    return [
        {
            "name": tool.name,
            "description": tool.description,
            "domain": tool.domain,
            "action": tool.action,
            "input_schema": tool.input_schema,
            "state": "registered",
            "source": "mcp_registry",
        }
        for name in registry.names()
        if (tool := registry.get(name)) is not None
    ]


def _collect_routes() -> list[dict[str, Any]]:
    return [
        {
            **route,
            "source": "gateway",
            "state": "declared",
        }
        for route in _INTERFACE_ROUTES
    ]


async def fetch_interface_inventory() -> dict[str, Any]:
    registry = build_default_registry()
    domains: list[dict[str, Any]] = []
    total_actions = 0

    for domain in registry.registered_domains():
        adapter = registry.get_adapter(domain)
        actions = adapter.describe_actions() if adapter is not None else []
        normalized_actions = []
        for action in actions:
            protocols = action.get("protocols") or ["rest", "mcp"]
            normalized_actions.append(
                {
                    "name": action.get("name"),
                    "description": action.get("description"),
                    "permission": action.get("permission") or Permission.SYSTEM_READ.value,
                    "protocols": protocols,
                    "state": "ready" if protocols else "pending",
                    "source": "adapter",
                }
            )
        total_actions += len(normalized_actions)
        domains.append(
            {
                "name": domain,
                "adapter": type(adapter).__name__ if adapter is not None else None,
                "actions": normalized_actions,
                "action_count": len(normalized_actions),
                "state": "ready" if normalized_actions else "pending",
                "source": "registry",
            }
        )

    proxies = await probe_infra_targets()
    proxy_targets = [
        {
            "name": name,
            "ok": payload.get("ok", False),
            "status_code": payload.get("status_code"),
            "url": payload.get("url"),
            "detail": payload.get("detail"),
            "state": "reachable" if payload.get("ok", False) else "degraded",
            "source": "probe",
        }
        for name, payload in proxies.get("targets", {}).items()
    ]

    routes = _collect_routes()
    mcp_tools = _collect_mcp_tools()

    return {
        "generated_at": _utc_now(),
        "domains": domains,
        "mcp_tools": mcp_tools,
        "routes": routes,
        "proxies": proxy_targets,
        "summary": {
            "domains": len(domains),
            "actions": total_actions,
            "mcp_tools": len(mcp_tools),
            "routes": len(routes),
            "proxies": len(proxy_targets),
            "reachable_proxies": sum(1 for proxy in proxy_targets if proxy.get("ok")),
        },
    }
