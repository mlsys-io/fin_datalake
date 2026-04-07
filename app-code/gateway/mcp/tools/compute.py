"""
MCP Tools: Compute Domain

Exposes ComputeAdapter capabilities as MCP Tools.

Registered Tools:
  - submit_job:    Trigger a named Prefect deployment.
  - get_job_status: Get the status of a running flow run.
"""

from mcp.server import Server
from mcp.types import Tool, TextContent

from gateway.core.registry import InterfaceRegistry, DomainNotFoundError
from gateway.core.adapters import ActionNotFoundError, PermissionError
from gateway.core.dispatch import dispatch, CircuitBreakerOpenError
from gateway.models.user import User


def register(server: Server, registry: InterfaceRegistry, user: User):
    """Register all Compute domain tools onto the MCP server."""

    @server.list_tools()
    async def list_compute_tools() -> list[Tool]:
        return [
            Tool(
                name="submit_job",
                description="Trigger a named ETL pipeline on the Ray/Prefect cluster.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "pipeline": {"type": "string", "description": "Name of the Prefect deployment."},
                        "params": {"type": "object", "description": "Optional flow run parameters."},
                    },
                    "required": ["pipeline"],
                }
            ),
            Tool(
                name="get_job_status",
                description="Get the status of a previously submitted pipeline job.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string", "description": "UUID of the Prefect flow run."}
                    },
                    "required": ["job_id"],
                }
            ),
        ]

    @server.call_tool()
    async def handle_compute_tool(name: str, arguments: dict) -> list[TextContent]:
        action_map = {
            "submit_job": "submit_job",
            "get_job_status": "get_status",
        }
        action = action_map.get(name)
        if action is None:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]

        try:
            result = await dispatch(
                registry=registry,
                user=user,
                domain="compute",
                action=action,
                parameters=arguments,
                source_protocol="mcp"
            )
            return [TextContent(type="text", text=str(result.data))]
        except (CircuitBreakerOpenError, PermissionError, DomainNotFoundError, ActionNotFoundError, ValueError) as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]
        except Exception as e:
            return [TextContent(type="text", text=f"Internal Error: {str(e)}")]
