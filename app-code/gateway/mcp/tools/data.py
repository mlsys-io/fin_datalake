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
  Each handler calls dispatch() which handles auth, tracing, and audit logging.
"""

from mcp.server import Server
from mcp.types import Tool, TextContent

from gateway.core.registry import InterfaceRegistry, DomainNotFoundError
from gateway.core.adapters import ActionNotFoundError, PermissionError
from gateway.core.dispatch import dispatch, CircuitBreakerOpenError
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
            return [TextContent(type="text", text=f"Unknown tool: {name}")]

        try:
            result = await dispatch(
                registry=registry,
                user=user,
                domain="data",
                action=action,
                parameters=arguments,
                source_protocol="mcp"
            )
            return [TextContent(type="text", text=str(result.data))]
        except (CircuitBreakerOpenError, PermissionError, DomainNotFoundError, ActionNotFoundError, ValueError) as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]
        except Exception as e:
            return [TextContent(type="text", text=f"Internal Error: {str(e)}")]
