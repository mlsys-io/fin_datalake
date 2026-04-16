from __future__ import annotations

from gateway.mcp.tool_registry import GatewayMcpTool


TOOL_SPECS = [
    GatewayMcpTool(
        name="query_system_logs",
        description="Query centralized system logs across all services.",
        input_schema={
            "type": "object",
            "properties": {
                "component": {
                    "type": "string",
                    "description": "Filter by component.",
                },
                "level": {
                    "type": "string",
                    "description": "Filter by level (INFO, ERROR, WARNING).",
                },
                "since": {
                    "type": "string",
                    "description": "Time window such as '1h' or '24h'.",
                },
                "agent_name": {
                    "type": "string",
                    "description": "Filter by specific agent name.",
                },
                "trace_id": {
                    "type": "string",
                    "description": "Filter by trace or request identifier.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of rows to return.",
                },
            },
        },
        domain="system",
        action="logs",
    ),
    GatewayMcpTool(
        name="get_system_health",
        description="Get the current health status of monitored components.",
        input_schema={"type": "object", "properties": {}},
        domain="system",
        action="health",
    ),
    GatewayMcpTool(
        name="get_overseer_snapshots",
        description="Retrieve recent overseer snapshots.",
        input_schema={
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of snapshots to return.",
                }
            },
        },
        domain="system",
        action="overseer_snapshots",
    ),
    GatewayMcpTool(
        name="get_overseer_alerts",
        description="Retrieve recent overseer alert events.",
        input_schema={
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of alerts to return.",
                }
            },
        },
        domain="system",
        action="overseer_alerts",
    ),
    GatewayMcpTool(
        name="get_infra_status",
        description="Probe infrastructure dashboard targets such as Prefect and Ray.",
        input_schema={"type": "object", "properties": {}},
        domain="system",
        action="infra_status",
    ),
    GatewayMcpTool(
        name="query_audit_logs",
        description="Query persisted gateway audit logs.",
        input_schema={
            "type": "object",
            "properties": {
                "since": {
                    "type": "string",
                    "description": "Time window such as '1h' or '24h'.",
                },
                "request_id": {
                    "type": "string",
                    "description": "Filter by gateway request identifier.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of rows to return.",
                },
                "source_protocol": {
                    "type": "string",
                    "description": "Filter by REST or MCP.",
                },
                "domain": {
                    "type": "string",
                    "description": "Filter by domain.",
                },
                "action": {
                    "type": "string",
                    "description": "Filter by action.",
                },
                "status_code": {
                    "type": "integer",
                    "description": "Filter by HTTP-style status code.",
                },
                "user_id": {
                    "type": "string",
                    "description": "Filter by user identifier.",
                },
            },
        },
        domain="system",
        action="audit_logs",
    ),
    GatewayMcpTool(
        name="get_interface_inventory",
        description="Inspect gateway interfaces, routes, and MCP tools.",
        input_schema={"type": "object", "properties": {}},
        domain="system",
        action="interface_inventory",
    ),
]


def get_tool_specs() -> list[GatewayMcpTool]:
    return list(TOOL_SPECS)
