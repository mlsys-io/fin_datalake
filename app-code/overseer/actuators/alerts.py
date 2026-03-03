"""
AlertActuator — Logs every action and optionally POSTs alerts to the Gateway.
"""

from __future__ import annotations

import logging

from overseer.actuators.base import BaseActuator
from overseer.models import ActionResult, OverseerAction

logger = logging.getLogger("overseer.actuators.alerts")


class AlertActuator(BaseActuator):

    def __init__(self, gateway_url: str | None = None):
        self.gateway_url = gateway_url

    async def execute(self, action: OverseerAction) -> ActionResult:
        alert = action.to_alert()
        logger.warning(f"[OVERSEER ALERT] {alert}")

        if self.gateway_url:
            try:
                import httpx
                async with httpx.AsyncClient(timeout=5.0) as client:
                    await client.post(f"{self.gateway_url}/api/v1/overseer/alerts", json=alert)
            except Exception as e:
                logger.error(f"Failed to push alert to Gateway: {e}")

        return ActionResult(success=True, detail=f"Alert logged: {action.reason}")
