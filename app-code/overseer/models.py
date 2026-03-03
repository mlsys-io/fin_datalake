"""
Overseer Data Models.

Defines the core value objects used throughout the Overseer:
  - ServiceEndpoint: connection config for a single service
  - ServiceMetrics:  standardized output from a Collector
  - SystemSnapshot:  a point-in-time view of all services
  - OverseerAction:  a decision made by a Policy
  - ActionResult:    outcome of executing an action
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Service Connection Config
# ---------------------------------------------------------------------------

@dataclass
class ServiceEndpoint:
    """
    Describes how to reach a single managed service.

    Loaded from config.yaml. The Overseer uses this to construct
    the appropriate Collector for each service.
    """
    name: str
    host: str = "localhost"
    port: int = 8080
    protocol: str = "http"          # "http", "kafka", "ray"
    health_path: str | None = None  # e.g. "/api/v0/health" for HTTP services
    extra: dict = field(default_factory=dict)  # Service-specific config

    @property
    def base_url(self) -> str:
        """Convenience: build 'http://host:port'."""
        return f"{self.protocol}://{self.host}:{self.port}"


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

@dataclass
class ServiceMetrics:
    """Standardized output from a Collector probe."""
    service: str
    healthy: bool
    data: dict = field(default_factory=dict)
    error: str | None = None
    collected_at: float = field(default_factory=time.time)


@dataclass
class SystemSnapshot:
    """A point-in-time view of the entire system."""
    services: dict[str, ServiceMetrics] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


# ---------------------------------------------------------------------------
# Actions
# ---------------------------------------------------------------------------

class ActionType(str, Enum):
    SCALE_UP = "scale_up"
    SCALE_DOWN = "scale_down"
    RESPAWN = "respawn"
    ALERT = "alert"


@dataclass
class OverseerAction:
    """A decision produced by a Policy, to be executed by an Actuator."""
    type: ActionType
    target: str = "ray"             # Which actuator handles this
    agent: str = ""                 # Agent class name (for scaling/respawn)
    count: int = 1
    reason: str = ""
    timestamp: float = field(default_factory=time.time)

    def to_alert(self) -> dict:
        return {
            "type": self.type.value,
            "agent": self.agent,
            "count": self.count,
            "reason": self.reason,
            "timestamp": self.timestamp,
        }


@dataclass
class ActionResult:
    """Outcome of executing an OverseerAction."""
    success: bool
    detail: str = ""
    error: str | None = None
