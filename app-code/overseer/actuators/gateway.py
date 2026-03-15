"""
Gateway Actuator — Controls the Gateway's backpressure/circuit-breaker.
"""

from __future__ import annotations

import httpx
import os
from loguru import logger

from overseer.actuators.base import BaseActuator
from overseer.models import ActionResult, OverseerAction


class GatewayActuator(BaseActuator):
    """
    Signals the Gateway to enable/disable backpressure.
    
    This is used during system overload or critical failures to
    protect the system by rejecting non-essential requests.
    """

    async def execute(self, action: OverseerAction) -> ActionResult:
        # Get Gateway internal URL
        gateway_url = os.getenv("GATEWAY_INTERNAL_URL")
        if not gateway_url:
            return ActionResult(success=False, error="GATEWAY_INTERNAL_URL not set")

        endpoint = f"{gateway_url}/api/v1/system/circuit-breaker"
        
        # Payload for the dedicated control endpoint
        payload = {
            "state": "open" if action.type.value == "circuit_break" else "closed",
            "ttl": action.count if action.count > 1 else 60,
            "reason": action.reason
        }

        token = os.getenv("GATEWAY_INTERNAL_TOKEN")
        if not token:
            return ActionResult(success=False, error="GATEWAY_INTERNAL_TOKEN not set")

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                # Internal machine-to-machine authentication
                headers = {"Authorization": f"Bearer {token}"}
                response = await client.post(endpoint, json=payload, headers=headers)
                
                if response.status_code < 400:
                    state_msg = payload['state'].upper()
                    return ActionResult(success=True, detail=f"Gateway circuit breaker set to {state_msg}ED")
                else:
                    return ActionResult(
                        success=False, 
                        error=f"Gateway rejected request: {response.status_code} - {response.text}"
                    )
        except Exception as e:
            logger.error(f"Failed to communicate with Gateway: {e}")
            return ActionResult(success=False, error=str(e))
