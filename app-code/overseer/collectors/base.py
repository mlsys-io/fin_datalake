"""
Base Collector — Abstract interface for service probes.

Every monitored service gets one Collector subclass that knows how to
connect via that service's native protocol and return ServiceMetrics.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

import httpx

from overseer.models import ServiceEndpoint, ServiceMetrics


class BaseCollector(ABC):
    """
    Probes a single service and returns standardized metrics.

    Subclasses implement `collect()` with service-specific logic.
    The default `is_healthy()` works for any HTTP service with a
    health endpoint configured in ServiceEndpoint.health_path.
    """

    def __init__(self, endpoint: ServiceEndpoint):
        self.endpoint = endpoint

    @abstractmethod
    async def collect(self) -> ServiceMetrics:
        """Probe the service and return structured metrics."""
        ...

    async def is_healthy(self) -> bool:
        """
        Quick liveness check via HTTP health endpoint.

        Override this for non-HTTP services (e.g. Kafka binary protocol).
        """
        if not self.endpoint.health_path:
            return True  # No health endpoint configured — assume healthy
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                url = f"http://{self.endpoint.host}:{self.endpoint.port}{self.endpoint.health_path}"
                r = await client.get(url)
                return r.status_code < 400
        except Exception:
            return False
