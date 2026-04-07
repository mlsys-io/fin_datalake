"""
BrokerAdapter: Domain "broker"

Handles the Connection Broker: vending direct-access credentials so
power users can bypass the Gateway for high-throughput I/O.

Supported Actions:
    - get_s3_creds:      Return MinIO/S3 connection credentials.
    - get_psql_string:   Return the Postgres/TimescaleDB connection string.
    - list_connections:  List available named services the broker can vend.

Design Note:
    BrokerAdapter has a stricter security posture than DataAdapter.
    DataAdapter EXECUTES queries on behalf of the user (gateway mediates).
    BrokerAdapter VENDS CREDENTIALS to the user (they bypass the gateway).

Required Permissions:
    - list_connections: broker:read  (metadata only)
    - get_s3_creds:     broker:vend  (returns sensitive credentials)
    - get_psql_string:  broker:vend  (returns sensitive credentials)
"""

import os
from typing import Any

from gateway.core.adapters import BaseAdapter, ActionNotFoundError
from gateway.core.rbac import Permission
from gateway.models.intent import UserIntent
from gateway.models.user import User


class BrokerAdapter(BaseAdapter):

    def handles(self) -> str:
        return "broker"

    async def execute(self, user: User, intent: UserIntent) -> Any:
        dispatch = {
            "get_s3_creds": self._get_s3_creds,
            "get_psql_string": self._get_psql_string,
            "list_connections": self._list_connections,
        }
        handler = dispatch.get(intent.action)
        if handler is None:
            raise ActionNotFoundError(
                f"BrokerAdapter does not support action '{intent.action}'. "
                f"Available: {list(dispatch.keys())}"
            )
        return handler(user, intent)

    def _get_s3_creds(self, user: User, intent: UserIntent) -> dict:
        """Vend MinIO/S3 credentials for direct access. Requires broker:vend."""
        self._require_permission(user, Permission.BROKER_VEND)
        # TODO: Integrate with AWS STS AssumeRole for time-limited tokens.
        return {
            "service": "minio",
            "endpoint_url": os.environ.get("MINIO_ENDPOINT"),
            "access_key_id": os.environ.get("AWS_ACCESS_KEY_ID"),
            "secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
            "region": os.environ.get("AWS_REGION", "us-east-1"),
            "note": "Use with any S3-compatible client (boto3, Cyberduck, etc.).",
        }

    def _get_psql_string(self, user: User, intent: UserIntent) -> dict:
        """Vend a TimescaleDB connection string. Requires broker:vend."""
        self._require_permission(user, Permission.BROKER_VEND)
        host = os.environ.get("TIMESCALE_HOST")
        port = os.environ.get("TIMESCALE_PORT", "5432")
        db = os.environ.get("TIMESCALE_DB", "etl")
        usr = os.environ.get("TIMESCALE_USER")
        pw = os.environ.get("TIMESCALE_PASSWORD")
        return {
            "service": "timescaledb",
            "connection_string": f"postgresql://{usr}:{pw}@{host}:{port}/{db}",
            "jdbc_url": f"jdbc:postgresql://{host}:{port}/{db}",
            "note": "Connect via DBeaver, Tableau, or psycopg2 directly.",
        }

    def _list_connections(self, user: User, intent: UserIntent) -> dict:
        """List available credential services. Requires broker:read."""
        self._require_permission(user, Permission.BROKER_READ)
        return {
            "available_connections": [
                {
                    "name": "minio",
                    "description": "S3-compatible object storage (Delta Lake, raw files)",
                    "action": "get_s3_creds",
                    "requires_permission": "broker:vend",
                },
                {
                    "name": "timescaledb",
                    "description": "TimescaleDB time-series database",
                    "action": "get_psql_string",
                    "requires_permission": "broker:vend",
                },
            ]
        }
