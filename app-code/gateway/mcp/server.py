from __future__ import annotations

import os

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent

from gateway.core.adapters import ActionNotFoundError, AdapterExecutionError, PermissionError
from gateway.core.dispatch import CircuitBreakerOpenError, dispatch
from gateway.core.registry import DomainNotFoundError, build_default_registry
from gateway.db import crud
from gateway.db.session import AsyncSessionLocal
from gateway.mcp.tool_registry import GatewayMcpToolRegistry, format_mcp_result
from gateway.mcp.tools import agent as agent_tools
from gateway.mcp.tools import broker as broker_tools
from gateway.mcp.tools import compute as compute_tools
from gateway.mcp.tools import data as data_tools
from gateway.mcp.tools import system as system_tools
from gateway.models.user import User


def _collect_tool_specs() -> GatewayMcpToolRegistry:
    registry = GatewayMcpToolRegistry()
    for module in (data_tools, compute_tools, agent_tools, system_tools, broker_tools):
        registry.register_many(module.get_tool_specs())
    return registry


async def create_mcp_server() -> Server:
    server = Server("lakehouse-gateway")

    from gateway.core.rbac import load_roles

    await load_roles()

    gateway_registry = build_default_registry()
    api_key = os.environ.get("GATEWAY_API_KEY", "")
    mcp_user = await _resolve_mcp_user(api_key)
    tool_registry = _collect_tool_specs()

    @server.list_tools()
    async def list_tools() -> list:
        return tool_registry.list_tools()

    @server.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[TextContent]:
        spec = tool_registry.get(name)
        if spec is None:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]

        try:
            result = await dispatch(
                registry=gateway_registry,
                user=mcp_user,
                domain=spec.domain,
                action=spec.action,
                parameters=spec.map_arguments(arguments),
                source_protocol="mcp",
            )
            return [TextContent(type="text", text=format_mcp_result(result.data))]
        except (CircuitBreakerOpenError, PermissionError, AdapterExecutionError, DomainNotFoundError, ActionNotFoundError, ValueError) as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]
        except Exception as e:
            return [TextContent(type="text", text=f"Internal Error: {str(e)}")]

    return server


async def _resolve_mcp_user(api_key: str) -> User:
    async with AsyncSessionLocal() as db:
        user = await crud.resolve_api_key(db, api_key)

    if not user:
        raise ValueError("Invalid GATEWAY_API_KEY. Authentication failed.")
    return user


async def main():
    server = await create_mcp_server()
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
