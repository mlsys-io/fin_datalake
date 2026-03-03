"""
MCP Interface Translator — /mcp

This package is the Model Context Protocol (MCP) Translator for the Gateway.
It exposes MCP Tools that AI assistants (Cursor, Claude) can invoke, translating
each tool call into a UserIntent routed through the InterfaceRegistry.

Protocol:
  MCP communicates over stdio (local process) or SSE (HTTP streaming).
  Each Tool is a named capability with a JSON schema for its arguments.

Package Structure:
  mcp/
  ├── server.py         — MCP server initialization & tool registration
  └── tools/
      ├── data.py       — Tools: query_data, list_tables, get_schema
      ├── compute.py    — Tools: submit_job, get_job_status
      └── agent.py      — Tools: chat_agent, list_agents
"""
