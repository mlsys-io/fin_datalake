"""
PrefectCollector — Probes the Prefect Server REST API.

Fetches recent flow runs and identifies failed/stuck runs.
"""

from __future__ import annotations

import httpx

from overseer.collectors.base import BaseCollector
from overseer.models import ServiceMetrics


class PrefectCollector(BaseCollector):

    async def collect(self) -> ServiceMetrics:
        base = f"http://{self.endpoint.host}:{self.endpoint.port}"
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                # Prefect 2.x uses POST to /api/flow_runs/filter
                r = await client.post(
                    f"{base}/api/flow_runs/filter",
                    json={
                        "sort": "EXPECTED_START_TIME_DESC",
                        "limit": 20,
                    },
                )
                if r.status_code != 200:
                    return ServiceMetrics(
                        service="prefect", healthy=True,
                        data={"note": f"Prefect API returned {r.status_code}"},
                    )
                runs = r.json()

            total = len(runs)
            failed = [run for run in runs if run.get("state_type") == "FAILED"]
            running = [run for run in runs if run.get("state_type") == "RUNNING"]

            return ServiceMetrics(
                service="prefect",
                healthy=True,
                data={
                    "recent_runs": total,
                    "running": len(running),
                    "failed": len(failed),
                    "failed_details": [
                        {"name": r.get("name"), "id": r.get("id")}
                        for r in failed[:5]
                    ],
                },
            )
        except Exception as e:
            return ServiceMetrics(service="prefect", healthy=False, error=str(e))
