"""
System-oriented gateway service helpers.

These helpers keep router code thin and centralize low-cost readiness and
observability probes used by the operator UI.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlparse, urlunparse

import httpx
from fastapi import FastAPI
from sqlalchemy import desc, func, select
from redis.asyncio import Redis as AsyncRedis

from gateway.core import config
from gateway.core.redis import get_redis_client
from gateway.db.audit_log import AuditLogORM
from gateway.db.session import AsyncSessionLocal
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
                    "ok": response.status_code < 400 or response.status_code in {401, 403},
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


def _parse_audit_parameters(value: str | None) -> Any:
    if not value:
        return {}
    try:
        return json.loads(value)
    except Exception:
        return value


def _parse_since_to_datetime(since: str | None) -> datetime:
    now = datetime.now(timezone.utc)
    if not since:
        return now - timedelta(hours=1)

    unit = since[-1].lower()
    try:
        amount = int(since[:-1])
    except (ValueError, IndexError):
        amount = 1
        unit = "h"

    if unit == "m":
        return now - timedelta(minutes=amount)
    if unit == "d":
        return now - timedelta(days=amount)
    return now - timedelta(hours=amount if amount > 0 else 1)


async def fetch_audit_logs(
    *,
    since: str | None = "1h",
    limit: int = 100,
    request_id: str | None = None,
    source_protocol: str | None = None,
    domain: str | None = None,
    action: str | None = None,
    status_code: int | None = None,
    user_id: str | None = None,
) -> dict[str, Any]:
    try:
        limit = max(1, min(int(limit), 500))
    except (TypeError, ValueError):
        limit = 100
    since_dt = _parse_since_to_datetime(since)

    conditions = [AuditLogORM.timestamp >= since_dt]
    if request_id:
        conditions.append(AuditLogORM.request_id == request_id)
    if source_protocol:
        conditions.append(AuditLogORM.source_protocol == source_protocol)
    if domain:
        conditions.append(AuditLogORM.domain == domain)
    if action:
        conditions.append(AuditLogORM.action == action)
    if status_code is not None:
        conditions.append(AuditLogORM.status_code == int(status_code))
    if user_id:
        conditions.append(AuditLogORM.user_id == user_id)

    stmt = (
        select(AuditLogORM)
        .where(*conditions)
        .order_by(desc(AuditLogORM.timestamp), desc(AuditLogORM.id))
        .limit(limit)
    )
    count_stmt = select(func.count()).select_from(AuditLogORM).where(*conditions)

    async with AsyncSessionLocal() as db:
        result = await db.execute(stmt)
        rows = result.scalars().all()
        count_result = await db.execute(count_stmt)
        total_count = int(count_result.scalar_one())

    logs = [
        {
            "id": row.id,
            "request_id": row.request_id,
            "timestamp": row.timestamp.isoformat() if row.timestamp else None,
            "user_id": row.user_id,
            "domain": row.domain,
            "action": row.action,
            "parameters": _parse_audit_parameters(row.parameters),
            "source_protocol": row.source_protocol,
            "status_code": row.status_code,
            "duration_ms": row.duration_ms,
            "error_detail": row.error_detail,
        }
        for row in rows
    ]

    return {
        "audit_logs": logs,
        "count": total_count,
        "returned_count": len(logs),
        "query": {
            "since": since,
            "limit": limit,
            "request_id": request_id,
            "source_protocol": source_protocol,
            "domain": domain,
            "action": action,
            "status_code": status_code,
            "user_id": user_id,
        },
    }


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
