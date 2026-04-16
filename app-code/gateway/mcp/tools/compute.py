from __future__ import annotations

from gateway.mcp.tool_registry import GatewayMcpTool


TOOL_SPECS = [
    GatewayMcpTool(
        name="submit_job",
        description="Trigger a named ETL pipeline on the compute plane.",
        input_schema={
            "type": "object",
            "properties": {
                "pipeline": {
                    "type": "string",
                    "description": "Name of the deployment or flow.",
                },
                "params": {
                    "type": "object",
                    "description": "Optional run parameters.",
                },
            },
            "required": ["pipeline"],
        },
        domain="compute",
        action="submit_job",
    ),
    GatewayMcpTool(
        name="get_job_status",
        description="Get the status of a running job or flow run.",
        input_schema={
            "type": "object",
            "properties": {
                "job_id": {
                    "type": "string",
                    "description": "Identifier of the flow run.",
                }
            },
            "required": ["job_id"],
        },
        domain="compute",
        action="get_status",
    ),
    GatewayMcpTool(
        name="list_jobs",
        description="List recent compute jobs visible to the gateway.",
        input_schema={
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of jobs to return.",
                }
            },
        },
        domain="compute",
        action="list_jobs",
    ),
    GatewayMcpTool(
        name="cancel_job",
        description="Cancel a running compute job.",
        input_schema={
            "type": "object",
            "properties": {
                "job_id": {
                    "type": "string",
                    "description": "Identifier of the flow run.",
                }
            },
            "required": ["job_id"],
        },
        domain="compute",
        action="cancel_job",
    ),
]


def get_tool_specs() -> list[GatewayMcpTool]:
    return list(TOOL_SPECS)
