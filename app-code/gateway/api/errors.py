"""
Shared API error envelope helpers for the gateway.
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException
from pydantic import BaseModel


class APIErrorResponse(BaseModel):
    detail: str
    code: str
    context: dict[str, Any] | None = None


def build_error_payload(
    *,
    detail: str,
    code: str,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return APIErrorResponse(detail=detail, code=code, context=context).model_dump()


def normalize_error_payload(detail: Any, *, default_code: str = "http_error") -> dict[str, Any]:
    if isinstance(detail, dict):
        message = str(detail.get("detail") or "Request failed.")
        code = str(detail.get("code") or default_code)
        context = detail.get("context")
        if context is not None and not isinstance(context, dict):
            context = {"value": context}
        return build_error_payload(detail=message, code=code, context=context)

    return build_error_payload(
        detail=str(detail or "Request failed."),
        code=default_code,
        context=None,
    )


def api_error(
    *,
    status_code: int,
    detail: str,
    code: str,
    context: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> HTTPException:
    return HTTPException(
        status_code=status_code,
        detail=build_error_payload(detail=detail, code=code, context=context),
        headers=headers,
    )
