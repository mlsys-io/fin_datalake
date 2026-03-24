"""
MCP Tools: System Domain

Exposes SystemAdapter capabilities as MCP Tools.

Registered Tools:
  - query_system_logs: Query centralized logs from TimescaleDB
  - get_system_health: Get the system health summary of all components
"""

from typing import Any

from mcp.server import Server
from mcp.types import Tool, TextContent

from gateway.core.registry import InterfaceRegistry
from gateway.models.intent import UserIntent
from gateway.models.user import User


def register(server: Server, registry: InterfaceRegistry, user: User):
    """Register all System domain tools onto the MCP server."""

    @server.list_tools()
    async def list_system_tools() -> list[Tool]:
        return [
            Tool(
                name="query_system_logs",
                description="Query centralized system logs from TimescaleDB across all services.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "component": {"type": "string", "description": "Filter by component (e.g. agent, hub, overseer)."},
                        "level": {"type": "string", "description": "Filter by level (INFO, ERROR, WARNING)."},
                        "since": {"type": "string", "description": "Time window (e.g. '1h', '24h'). Default: 1h."},
                        "limit": {"type": "integer", "description": "Max rows to return. Default: 100."},
                    },
                    "required": [],
                }
            ),
            Tool(
                name="get_system_health",
                description="Get the current health status of all monitored system components.",
                inputSchema={"type": "object", "properties": {}},
            ),
        ]

    @server.call_tool()
    async def handle_system_tool(name: str, arguments: dict) -> list[TextContent]:
        action_map = {
            "query_system_logs": "logs",
            "get_system_health": "health",
        }
        action = action_map.get(name)
        if action is None:
            return []

        intent = UserIntent(
            domain="system",
            action=action,
            parameters=arguments,
            user_id=user.username,
            roles=user.role_names if user.role_names else ["analyst"],
        )

        result = await registry.route(user, intent)
        return [TextContent(type="text", text=str(result))]
