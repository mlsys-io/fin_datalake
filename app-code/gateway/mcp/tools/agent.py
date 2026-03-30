"""
MCP Tools: Agent Domain

Exposes AgentAdapter capabilities as MCP Tools.

Registered Tools:
  - chat_agent:   Send a message to a named AI Agent and receive a response.
  - list_agents:  List all available registered agents.
"""

from mcp.server import Server
from mcp.types import Tool, TextContent

from gateway.core.registry import InterfaceRegistry, DomainNotFoundError
from gateway.core.adapters import ActionNotFoundError, PermissionError
from gateway.core.dispatch import dispatch, CircuitBreakerOpenError
from gateway.models.user import User


def register(server: Server, registry: InterfaceRegistry, user: User):
    """Register all Agent domain tools onto the MCP server."""

    @server.list_tools()
    async def list_agent_tools() -> list[Tool]:
        return [
            Tool(
                name="chat_agent",
                description=(
                    "Send a message to a named AI Agent running in the Ray cluster "
                    "and receive its response."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "agent_name": {"type": "string", "description": "Name of the registered agent."},
                        "message": {"type": "string", "description": "The message or question to send."},
                    },
                    "required": ["agent_name", "message"],
                }
            ),
            Tool(
                name="list_agents",
                description="List all currently registered agents and their capabilities.",
                inputSchema={"type": "object", "properties": {}},
            ),
        ]

    @server.call_tool()
    async def handle_agent_tool(name: str, arguments: dict) -> list[TextContent]:
        action_map = {
            "chat_agent": "chat",
            "list_agents": "list",
        }
        action = action_map.get(name)
        if action is None:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]

        try:
            result = await dispatch(
                registry=registry,
                user=user,
                domain="agent",
                action=action,
                parameters=arguments,
                source_protocol="mcp"
            )
            return [TextContent(type="text", text=str(result.data))]
        except (CircuitBreakerOpenError, PermissionError, DomainNotFoundError, ActionNotFoundError, ValueError) as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]
        except Exception as e:
            return [TextContent(type="text", text=f"Internal Error: {str(e)}")]
