from __future__ import annotations

from gateway.mcp.tool_registry import GatewayMcpTool


TOOL_SPECS = [
    GatewayMcpTool(
        name="list_connections",
        description="List available credential services.",
        input_schema={"type": "object", "properties": {}},
        domain="broker",
        action="list_connections",
    ),
    GatewayMcpTool(
        name="get_s3_creds",
        description="Vend MinIO/S3 credentials for direct object storage access.",
        input_schema={"type": "object", "properties": {}},
        domain="broker",
        action="get_s3_creds",
    ),
    GatewayMcpTool(
        name="get_psql_string",
        description="Vend a TimescaleDB / PostgreSQL connection string.",
        input_schema={"type": "object", "properties": {}},
        domain="broker",
        action="get_psql_string",
    ),
]


def get_tool_specs() -> list[GatewayMcpTool]:
    return list(TOOL_SPECS)
