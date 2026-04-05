"""
System-oriented gateway service helpers.

These helpers keep router code thin and centralize low-cost readiness and
observability probes used by the operator UI.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse, urlunparse

import httpx
from fastapi import FastAPI
from redis.asyncio import Redis as AsyncRedis

from gateway.core import config
from gateway.core.redis import get_redis_client
from gateway.db.session import engine


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_overseer_redis_url() -> str:
    parsed = urlparse(config.REDIS_URL)
    return urlunparse(parsed._replace(path="/1"))


async def _fetch_overseer_list(key: str, n: int) -> list[Any]:
    if not config.REDIS_URL:
        raise RuntimeError("Redis not configured")

    r = AsyncRedis.from_url(_build_overseer_redis_url(), decode_responses=True)
    async with r:
        items = await r.lrange(key, 0, n - 1)
    return items


async def fetch_overseer_snapshots(n: int = 50) -> list[dict[str, Any]]:
    items = await _fetch_overseer_list("overseer:snapshots", n)
    result: list[dict[str, Any]] = []
    for item in reversed(items):
        try:
            snap = json.loads(item)
        except Exception:
            continue
        result.append(
            {
                "timestamp": snap.get("timestamp"),
                "services": snap.get("services", {}),
            }
        )
    return result


async def fetch_overseer_alerts(n: int = 20) -> list[dict[str, Any]]:
    items = await _fetch_overseer_list("overseer:alerts", n)
    result: list[dict[str, Any]] = []
    for item in items:
        try:
            result.append(json.loads(item))
        except Exception:
            continue
    return result


async def probe_infra_targets() -> dict[str, Any]:
    targets = {
        "prefect": os.environ.get("PREFECT_UI_URL", "http://127.0.0.1:4200"),
        "ray": os.environ.get("RAY_DASHBOARD_URL", "http://127.0.0.1:32382"),
        "minio": os.environ.get("MINIO_CONSOLE_URL", "http://127.0.0.1:9001"),
    }

    async with httpx.AsyncClient(timeout=2.0, follow_redirects=True) as client:
        results: dict[str, Any] = {}
        for name, url in targets.items():
            try:
                response = await client.get(url)
                results[name] = {
                    "ok": response.status_code < 500,
                    "status_code": response.status_code,
                    "url": url,
                    "detail": None,
                }
            except Exception as exc:
                results[name] = {
                    "ok": False,
                    "status_code": None,
                    "url": url,
                    "detail": str(exc),
                }

    return {"targets": results}


async def check_db_readiness() -> dict[str, Any]:
    try:
        async with engine.connect() as conn:
            await conn.exec_driver_sql("SELECT 1")
        return {"ready": True, "detail": None}
    except Exception as exc:
        return {"ready": False, "detail": str(exc)}


async def check_redis_readiness() -> dict[str, Any]:
    if not config.REDIS_URL:
        return {
            "configured": False,
            "ready": False,
            "detail": "Redis URL is not configured.",
        }

    try:
        client = get_redis_client()
        async with client:
            await client.ping()
        return {
            "configured": True,
            "ready": True,
            "detail": None,
        }
    except Exception as exc:
        return {
            "configured": True,
            "ready": False,
            "detail": str(exc),
        }


async def build_readiness_report(app: FastAPI) -> dict[str, Any]:
    db = await check_db_readiness()
    redis = await check_redis_readiness()
    registry_ready = getattr(app.state, "registry", None) is not None
    ray_ready = bool(getattr(app.state, "ray_ready", False))

    checks = {
        "database": db,
        "registry": {
            "ready": registry_ready,
            "detail": None if registry_ready else "Gateway registry is not initialized.",
        },
        "ray": {
            "ready": ray_ready,
            "detail": None if ray_ready else "Gateway Ray client is not initialized.",
        },
        "redis": redis,
    }

    overall_ready = (
        checks["database"]["ready"]
        and checks["registry"]["ready"]
        and checks["ray"]["ready"]
        and (not redis["configured"] or redis["ready"])
    )

    return {
        "ready": overall_ready,
        "timestamp": _utc_now(),
        "checks": checks,
    }
