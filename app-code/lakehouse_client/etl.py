"""
Public ETL-facing API for users building on the platform.

This module re-exports the core task, service, runtime, and I/O primitives that
external users are expected to import when writing pipelines and integrations.
"""

from etl.config import Config, config
from etl.core.base_service import ServiceTask, SyncHandle
from etl.io.base import DataReader, DataSink, DataSource, DataWriter
from etl.runtime import (
    DEFAULT_WORKING_DIR_EXCLUDES,
    build_runtime_env,
    ensure_ray,
    resolve_ray_namespace,
    resolve_serve_response,
    resolve_working_dir,
)

try:
    from etl.core.base_task import BaseTask
except ModuleNotFoundError as exc:
    if exc.name == "prefect":
        raise ModuleNotFoundError(
            "lakehouse_client.etl requires Prefect. Install the client dependencies "
            "with `pip install -r requirements-client.txt` or the package client extra."
        ) from exc
    raise

__all__ = [
    "BaseTask",
    "Config",
    "DEFAULT_WORKING_DIR_EXCLUDES",
    "DataReader",
    "DataSink",
    "DataSource",
    "DataWriter",
    "ServiceTask",
    "SyncHandle",
    "build_runtime_env",
    "config",
    "ensure_ray",
    "resolve_ray_namespace",
    "resolve_serve_response",
    "resolve_working_dir",
]
