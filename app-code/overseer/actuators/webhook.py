"""
WebhookActuator — Routes Overseer actions to HTTP webhooks and Redis for live streaming.
"""
import dataclasses
import httpx
import json
from loguru import logger
from typing import Optional

from overseer.actuators.base import BaseActuator
from overseer.models import OverseerAction, ActionResult
from overseer.redis_utils import get_redis_client


class WebhookActuator(BaseActuator):
    """
    Sends alerts to external Webhooks (e.g. Discord, Slack) and 
    publishes them to a Redis channel for the dashboard SSE stream.
    """

    def __init__(self, webhook_url: Optional[str] = None):
        self.webhook_url = webhook_url
        self.redis = get_redis_client()
        if self.webhook_url:
            logger.info(f"WebhookActuator configured with destination: {self.webhook_url}")
        else:
            logger.warning("WebhookActuator initialized without a webhook_url. Will only route to Redis.")

    async def execute(self, action: OverseerAction) -> ActionResult:
        message = f"🚨 Overseer Action: {action.type.value} on {action.agent} | Reason: {action.reason}"
        payload = {"text": message}
        
        # 1. Publish to Redis for Dashboard (system:alerts channel)
        if self.redis:
            action_json = json.dumps(dataclasses.asdict(action))
            try:
                await self.redis.publish("system:alerts", action_json)
                logger.debug(f"Published action to system:alerts Redis channel")
            except Exception as e:
                logger.error(f"Failed to publish to Redis: {e}")

        # 2. Publish to Webhook
        if self.webhook_url:
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.post(self.webhook_url, json=payload, timeout=5.0)
                    if response.status_code >= 400:
                         logger.error(f"Webhook returned {response.status_code}: {response.text}")
            except Exception as e:
                logger.error(f"Failed to send webhook: {e}")

        return ActionResult(success=True, detail=f"Webhook/Redis routing complete for {action.type.value}.")
