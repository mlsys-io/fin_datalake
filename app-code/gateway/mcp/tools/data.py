"""
MCP Tools: Data Domain

Exposes the DataAdapter's capabilities (data:read permission) as MCP Tools.

Registered Tools:
  - query_data:  Execute SQL against Delta Lake.
  - list_tables: List available Delta tables.
  - get_schema:  Get the schema of a specific table.

How register() works:
  Called by server.py at startup.
  Uses the @server.call_tool() decorator to bind tool names to handlers.
  Each handler builds a UserIntent and calls registry.route(user, intent).
"""

from typing import Any

from mcp.server import Server
from mcp.types import Tool, TextContent

from gateway.core.registry import InterfaceRegistry
from gateway.models.intent import UserIntent
from gateway.models.user import User


def register(server: Server, registry: InterfaceRegistry, user: User):
    """Register all Data domain tools onto the MCP server."""

    @server.list_tools()
    async def list_data_tools() -> list[Tool]:
        return [
            Tool(
                name="query_data",
                description="Execute a SQL query against the Delta Lake Lakehouse.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "sql": {"type": "string", "description": "The SQL query to run."}
                    },
                    "required": ["sql"],
                }
            ),
            Tool(
                name="list_tables",
                description="List all available tables in the Lakehouse.",
                inputSchema={"type": "object", "properties": {}},
            ),
            Tool(
                name="get_schema",
                description="Get the schema (columns and types) of a specific Delta table.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "table_path": {"type": "string", "description": "S3 URI of the Delta table."}
                    },
                    "required": ["table_path"],
                }
            ),
        ]

    @server.call_tool()
    async def handle_data_tool(name: str, arguments: dict) -> list[TextContent]:
        """
        Handle MCP tool calls for the data domain.
        Translates MCP tool calls into UserIntents and routes them.
        """
        # Map MCP tool name → intent action
        action_map = {
            "query_data": "run_sql",
            "list_tables": "list_tables",
            "get_schema": "get_schema",
        }
        action = action_map.get(name)
        if action is None:
            return []

        intent = UserIntent(
            domain="data",
            action=action,
            parameters=arguments,
            user_id=user.username,
            roles=user.role_names if user.role_names else ["analyst"],
        )

        result = registry.route(user, intent)
        return [TextContent(type="text", text=str(result))]
