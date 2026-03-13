"""
MCP Server: The Model Context Protocol Translator.

Exposes the Gateway's capabilities as MCP Tools that can be invoked
by AI assistants (Cursor, Claude, LLM agents) via:
  - stdio  (local subprocess — standard for Cursor extensions)
  - SSE    (HTTP streaming — for remote MCP servers)

Architecture:
  server.py
  └── Initializes the `mcp.Server`.
  └── Imports and registers all tool modules:
        tools/data.py    — query_data, list_tables, get_schema
        tools/compute.py — submit_job, get_job_status
        tools/agent.py   — chat_agent, list_agents

How Each Tool Works:
  AI Assistant → MCP call_tool("query_data", {"sql": "..."})
  → server.py dispatches to tools/data.py
  → tools/data.py builds a UserIntent
  → routes through InterfaceRegistry
  → returns result to assistant

Auth:
  MCP connections are from trusted processes (Cursor, Claude Desktop).
  Authentication uses an API Key stored in the MCP client config.
  The key is resolved to a User on each tool call.

Usage:
    # Launch as stdio server (for Cursor MCP extension):
    python -m gateway.mcp.server

    # The MCP client config in Cursor will look like:
    # {
    #   "mcpServers": {
    #       "lakehouse": {
    #           "command": "python",
    #           "args": ["-m", "gateway.mcp.server"],
    #           "env": {"GATEWAY_API_KEY": "etl_sk_..."}
    #       }
    #   }
    # }
"""

import os
from mcp.server import Server
from mcp.server.stdio import stdio_server

# -- Tool Modules --
from gateway.mcp.tools import data as data_tools
from gateway.mcp.tools import compute as compute_tools
from gateway.mcp.tools import agent as agent_tools

# -- Registry & Auth --
from gateway.core.registry import build_default_registry
from gateway.models.user import User


async def create_mcp_server() -> Server:
    """
    Initialize the MCP server and register all tool handlers.

    Returns:
        A configured mcp.Server ready to be run over stdio or SSE.
    """
    server = Server("lakehouse-gateway")
    registry = build_default_registry()

    # Resolve the MCP client's User from env API Key.
    # MCP connections are from trusted local processes; we resolve once.
    api_key = os.environ.get("GATEWAY_API_KEY", "")
    mcp_user = await _resolve_mcp_user(api_key)

    # Register tool groups — each module registers its own tools on the server
    data_tools.register(server, registry, mcp_user)
    compute_tools.register(server, registry, mcp_user)
    agent_tools.register(server, registry, mcp_user)

    return server


async def _resolve_mcp_user(api_key: str) -> User:
    """
    Resolve the MCP client's identity from an API Key via DB lookup.
    """
    from gateway.db.session import AsyncSessionLocal
    from gateway.db import crud

    # Run the database resolution just like deps.py
    async with AsyncSessionLocal() as db:
        user = await crud.resolve_api_key(db, api_key)
        
    if not user:
        raise ValueError("Invalid GATEWAY_API_KEY. Authentication failed.")
    return user


async def main():
    """Entry point for running the MCP server over stdio."""
    server = await create_mcp_server()
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
