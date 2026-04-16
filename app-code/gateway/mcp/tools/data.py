from __future__ import annotations

from gateway.mcp.tool_registry import GatewayMcpTool


TOOL_SPECS = [
    GatewayMcpTool(
        name="query_data",
        description="Execute a read-only SQL query against the lakehouse.",
        input_schema={
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "The SQL query to run."},
            },
            "required": ["sql"],
        },
        domain="data",
        action="run_sql",
    ),
    GatewayMcpTool(
        name="query_stream",
        description="Execute a read-only SQL query against RisingWave.",
        input_schema={
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "The SQL query to run."},
            },
            "required": ["sql"],
        },
        domain="data",
        action="query_stream",
    ),
    GatewayMcpTool(
        name="list_tables",
        description="List all available tables in the catalog.",
        input_schema={"type": "object", "properties": {}},
        domain="data",
        action="list_tables",
    ),
    GatewayMcpTool(
        name="get_schema",
        description="Get the schema for a specific Delta table.",
        input_schema={
            "type": "object",
            "properties": {
                "table_path": {
                    "type": "string",
                    "description": "Path or URI of the Delta table.",
                }
            },
            "required": ["table_path"],
        },
        domain="data",
        action="get_schema",
    ),
    GatewayMcpTool(
        name="preview_table",
        description="Preview the first rows of a table through the gateway.",
        input_schema={
            "type": "object",
            "properties": {
                "table_path": {
                    "type": "string",
                    "description": "Path or URI of the table.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max rows to return.",
                },
            },
            "required": ["table_path"],
        },
        domain="data",
        action="preview",
    ),
    GatewayMcpTool(
        name="list_catalog_sources",
        description="Return grouped catalog sources discovered by the gateway.",
        input_schema={"type": "object", "properties": {}},
        domain="data",
        action="catalog_sources",
    ),
]


def get_tool_specs() -> list[GatewayMcpTool]:
    return list(TOOL_SPECS)
