"""
Server-Sent Events (SSE) router for real-time dashboard notifications.
"""

import asyncio
import json
from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from gateway.core.redis import get_redis_client

router = APIRouter()


async def alert_generator():
    """
    Subscribes to the 'system:alerts' Redis channel and yields Events.
    """
    redis = get_redis_client()
    if not redis:
        # Fallback if Redis is not configured, stream a 503-like message or simply wait.
        yield "data: {\"error\": \"Redis not connected\"}\n\n"
        while True:
            await asyncio.sleep(60)

    pubsub = redis.pubsub()
    await pubsub.subscribe("system:alerts")

    # Keep a heartbeat to avoid connection drops by proxies
    try:
        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=15.0)
            if message:
                data = message.get("data")
                if data:
                    yield f"data: {data}\n\n"
            else:
                # Heartbeat
                yield ": heartbeat\n\n"
    except asyncio.CancelledError:
        pass
    finally:
        await pubsub.unsubscribe("system:alerts")


@router.get("/alerts", summary="Stream Overseer Actions (SSE)")
async def stream_alerts():
    """
    Streams a continuous Server-Sent Events (SSE) feed of live Overseer 
    actions and system alerts directly to the frontend interface.
    """
    return StreamingResponse(
        alert_generator(),
        media_type="text/event-stream"
    )
