"""
UserIntent: The Universal Request Model.

This is the canonical "language" of the Gateway. Every incoming request
(from REST, MCP, CLI, or SDK) is translated into a UserIntent before being
routed to the appropriate Adapter.

Design Principle:
    Protocol-Agnostic. The adapters and registry never care about how the
    request arrived — they only care about the intent itself.
"""

from pydantic import BaseModel, Field
from typing import Any, Dict, Optional


class UserIntent(BaseModel):
    """
    A normalized, protocol-agnostic representation of a user action.

    The registry uses `domain` to find the correct adapter.
    The adapter uses `action` to find the correct internal method.

    Examples:
        # Query data via REST API
        UserIntent(domain="data", action="run_sql", parameters={"sql": "SELECT * FROM ohlc LIMIT 10"}, ...)

        # Submit a job via CLI
        UserIntent(domain="compute", action="submit_job", parameters={"pipeline": "kafka_to_delta"}, ...)

        # Chat with an agent via MCP
        UserIntent(domain="agent", action="chat", parameters={"agent_name": "MarketAnalyst", "message": "Buy or sell?"}, ...)

        # Get storage credentials via SDK (Connection Broker)
        UserIntent(domain="broker", action="get_s3_creds", parameters={"bucket": "delta-lake"}, ...)
    """

    domain: str = Field(
        ...,
        description="The subsystem to target. One of: 'data', 'compute', 'agent', 'broker'.",
        examples=["data", "compute", "agent", "broker"],
    )
    action: str = Field(
        ...,
        description="The specific operation to perform within the domain.",
        examples=["run_sql", "submit_job", "chat", "get_s3_creds"],
    )
    parameters: Dict[str, Any] = Field(
        default_factory=dict,
        description="Action-specific arguments.",
    )
    user_id: str = Field(
        ...,
        description="The identity of the requesting user (from Auth layer).",
    )
    role: str = Field(
        ...,
        description="The RBAC role of the requesting user: 'admin', 'engineer', 'analyst'.",
        examples=["admin", "engineer", "analyst"],
    )
    request_id: Optional[str] = Field(
        default=None,
        description="Optional correlation ID for distributed tracing / audit log.",
    )
