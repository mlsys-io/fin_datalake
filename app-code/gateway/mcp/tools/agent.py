from __future__ import annotations

from gateway.mcp.tool_registry import GatewayMcpTool


TOOL_SPECS = [
    GatewayMcpTool(
        name="chat_agent",
        description="Send a message to a named AI agent and receive its response.",
        input_schema={
            "type": "object",
            "properties": {
                "agent_name": {
                    "type": "string",
                    "description": "Name of the registered agent.",
                },
                "message": {
                    "type": "string",
                    "description": "The message or question to send.",
                },
                "session_id": {
                    "type": "string",
                    "description": "Optional conversation session identifier.",
                },
            },
            "required": ["agent_name", "message"],
        },
        domain="agent",
        action="chat",
    ),
    GatewayMcpTool(
        name="invoke_agent",
        description="Invoke a named agent with an arbitrary payload.",
        input_schema={
            "type": "object",
            "properties": {
                "agent_name": {
                    "type": "string",
                    "description": "Name of the registered agent.",
                },
                "payload": {
                    "description": "Payload to send to the agent.",
                },
                "session_id": {
                    "type": "string",
                    "description": "Optional conversation session identifier.",
                },
            },
            "required": ["agent_name", "payload"],
        },
        domain="agent",
        action="invoke",
    ),
    GatewayMcpTool(
        name="list_agents",
        description="List all currently registered agents and their capabilities.",
        input_schema={"type": "object", "properties": {}},
        domain="agent",
        action="list",
    ),
    GatewayMcpTool(
        name="broadcast_agents",
        description="Broadcast an event to all alive agents.",
        input_schema={
            "type": "object",
            "properties": {
                "payload": {
                    "type": "object",
                    "description": "Broadcast payload.",
                }
            },
            "required": ["payload"],
        },
        domain="agent",
        action="notify",
    ),
]


def get_tool_specs() -> list[GatewayMcpTool]:
    return list(TOOL_SPECS)
