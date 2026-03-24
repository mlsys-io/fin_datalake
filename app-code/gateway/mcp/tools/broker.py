"""
MCP Tools: Broker Domain

Exposes BrokerAdapter capabilities as MCP Tools.

Registered Tools:
  - list_connections: List available services that the broker can vend credentials for.
  - get_s3_creds: Get MinIO/S3 connection credentials.
  - get_psql_string: Get a TimescaleDB connection string.
"""

from typing import Any

from mcp.server import Server
from mcp.types import Tool, TextContent

from gateway.core.registry import InterfaceRegistry
from gateway.models.intent import UserIntent
from gateway.models.user import User


def register(server: Server, registry: InterfaceRegistry, user: User):
    """Register all Broker domain tools onto the MCP server."""

    @server.list_tools()
    async def list_broker_tools() -> list[Tool]:
        return [
            Tool(
                name="list_connections",
                description="List available credential services that the broker can vend.",
                inputSchema={"type": "object", "properties": {}},
            ),
            Tool(
                name="get_s3_creds",
                description="Vend MinIO/S3 credentials for direct object storage access.",
                inputSchema={"type": "object", "properties": {}},
            ),
            Tool(
                name="get_psql_string",
                description="Vend a TimescaleDB / PostgreSQL connection string.",
                inputSchema={"type": "object", "properties": {}},
            ),
        ]

    @server.call_tool()
    async def handle_broker_tool(name: str, arguments: dict) -> list[TextContent]:
        action_map = {
            "list_connections": "list_connections",
            "get_s3_creds": "get_s3_creds",
            "get_psql_string": "get_psql_string",
        }
        action = action_map.get(name)
        if action is None:
            return []

        intent = UserIntent(
            domain="broker",
            action=action,
            parameters=arguments,
            user_id=user.username,
            roles=user.role_names if user.role_names else ["analyst"],
        )

        result = await registry.route(user, intent)
        return [TextContent(type="text", text=str(result))]
