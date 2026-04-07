"""
GenericHealthCollector — Simple HTTP health check for any service.

Used for services that don't need deep metric collection (e.g. MinIO, Gateway).
Just checks if the health endpoint returns a 2xx/3xx response.
"""

from __future__ import annotations

from overseer.collectors.base import BaseCollector
from overseer.models import ServiceMetrics


class GenericHealthCollector(BaseCollector):

    async def collect(self) -> ServiceMetrics:
        healthy, error_msg = await self.is_healthy()
        return ServiceMetrics(
            service=self.endpoint.name,
            healthy=healthy,
            data={"check": "http_health", "endpoint": self.endpoint.health_path or "none"},
            error=error_msg if not healthy else None,
        )
