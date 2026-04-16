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
from urllib.parse import quote
from typing import Any

from gateway.core.adapters import AdapterExecutionError, BaseAdapter, ActionNotFoundError
from gateway.core.rbac import Permission
from gateway.models.intent import UserIntent
from gateway.models.user import User


class BrokerAdapter(BaseAdapter):

    def handles(self) -> str:
        return "broker"

    def describe_actions(self) -> list[dict[str, Any]]:
        return [
            {
                "name": "get_s3_creds",
                "description": "Return direct-access object storage credentials.",
                "permission": Permission.BROKER_VEND.value,
                "protocols": ["rest", "mcp"],
            },
            {
                "name": "get_psql_string",
                "description": "Return a PostgreSQL / TimescaleDB connection string.",
                "permission": Permission.BROKER_VEND.value,
                "protocols": ["rest", "mcp"],
            },
            {
                "name": "list_connections",
                "description": "List the connection services the broker can vend.",
                "permission": Permission.BROKER_READ.value,
                "protocols": ["rest", "mcp"],
            },
        ]

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

    def _require_broker_config(
        self,
        *,
        label: str,
        required_env: tuple[str, ...],
        optional_env: tuple[str, ...] = (),
    ) -> dict[str, str]:
        config: dict[str, str] = {}
        missing: list[str] = []

        for name in required_env:
            value = os.environ.get(name, "").strip()
            if value:
                config[name] = value
            else:
                missing.append(name)

        for name in optional_env:
            value = os.environ.get(name, "").strip()
            if value:
                config[name] = value

        if missing:
            raise AdapterExecutionError(
                f"{label} is not configured. Missing environment variables: {', '.join(missing)}",
                error_type="ConfigurationError",
                context={"missing": missing},
            )

        return config

    @staticmethod
    def _build_postgres_connection_string(*, user: str, password: str, host: str, port: str, database: str) -> str:
        return (
            "postgresql://"
            f"{quote(user, safe='')}:{quote(password, safe='')}@"
            f"{host}:{port}/{quote(database, safe='')}"
        )

    def _get_s3_creds(self, user: User, intent: UserIntent) -> dict:
        """Vend MinIO/S3 credentials for direct access. Requires broker:vend."""
        self._require_permission(user, Permission.BROKER_VEND)
        config = self._require_broker_config(
            label="MinIO/S3 credentials",
            required_env=("MINIO_ENDPOINT", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"),
            optional_env=("AWS_REGION",),
        )
        # TODO: Integrate with AWS STS AssumeRole for time-limited tokens.
        return {
            "service": "minio",
            "endpoint_url": config["MINIO_ENDPOINT"],
            "access_key_id": config["AWS_ACCESS_KEY_ID"],
            "secret_access_key": config["AWS_SECRET_ACCESS_KEY"],
            "region": config.get("AWS_REGION", "us-east-1"),
            "note": "Use with any S3-compatible client (boto3, Cyberduck, etc.).",
        }

    def _get_psql_string(self, user: User, intent: UserIntent) -> dict:
        """Vend a TimescaleDB connection string. Requires broker:vend."""
        self._require_permission(user, Permission.BROKER_VEND)
        config = self._require_broker_config(
            label="TimescaleDB connection details",
            required_env=("TIMESCALE_HOST", "TIMESCALE_USER", "TIMESCALE_PASSWORD"),
            optional_env=("TIMESCALE_PORT", "TIMESCALE_DB"),
        )
        host = config["TIMESCALE_HOST"]
        port = config.get("TIMESCALE_PORT", "5432")
        db = config.get("TIMESCALE_DB", "etl")
        usr = config["TIMESCALE_USER"]
        pw = config["TIMESCALE_PASSWORD"]
        try:
            port_number = int(port)
        except ValueError as exc:
            raise AdapterExecutionError(
                f"TIMESCALE_PORT must be a valid integer, got {port!r}.",
                error_type="ConfigurationError",
                context={"TIMESCALE_PORT": port},
            ) from exc
        if not 1 <= port_number <= 65535:
            raise AdapterExecutionError(
                f"TIMESCALE_PORT must be between 1 and 65535, got {port_number}.",
                error_type="ConfigurationError",
                context={"TIMESCALE_PORT": port_number},
            )
        return {
            "service": "timescaledb",
            "connection_string": self._build_postgres_connection_string(
                user=usr,
                password=pw,
                host=host,
                port=str(port_number),
                database=db,
            ),
            "jdbc_url": f"jdbc:postgresql://{host}:{port_number}/{quote(db, safe='')}",
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
