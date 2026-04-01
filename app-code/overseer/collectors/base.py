"""
Base Collector — Abstract interface for service probes.

Every monitored service gets one Collector subclass that knows how to
connect via that service's native protocol and return ServiceMetrics.
"""

from __future__ import annotations

import os
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

    async def is_healthy(self) -> tuple[bool, str | None]:
        """
        Quick liveness check via HTTP health endpoint.

        Returns (True, None) if healthy, or (False, error_message) if not.

        Note: httpx does NOT automatically read AWS_CA_BUNDLE or REQUESTS_CA_BUNDLE.
        We explicitly pass the cert path so self-signed certs (e.g. MinIO) are trusted.
        """
        if not self.endpoint.health_path:
            return True, None
        # Resolve the SSL verify parameter:
        #   - If a CA bundle path is set, use it (trusts self-signed certs like MinIO).
        #   - If OVERSEER_SSL_VERIFY=false is explicitly set, skip verification.
        #   - Otherwise default to True (full verification).
        ssl_verify: bool | str = True
        ca_bundle = os.environ.get("AWS_CA_BUNDLE") or os.environ.get("REQUESTS_CA_BUNDLE")
        if os.environ.get("OVERSEER_SSL_VERIFY", "").lower() == "false":
            ssl_verify = False
        elif ca_bundle:
            ssl_verify = ca_bundle

        try:
            async with httpx.AsyncClient(timeout=5.0, verify=ssl_verify) as client:
                url = f"{self.endpoint.base_url}{self.endpoint.health_path}"
                r = await client.get(url)
                if r.status_code >= 400:
                    return False, f"HTTP {r.status_code}"
                return True, None
        except Exception as e:
            return False, str(e)

