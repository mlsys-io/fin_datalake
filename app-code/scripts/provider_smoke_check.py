from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any, Dict, List

import requests
from loguru import logger

from etl.agents.langchain_adapter import LangChainMixin
from pipelines.market_pulse_ingest import (
    DEFAULT_SYMBOL,
    DEFAULT_WEBSOCKET_URL,
    FMP_API_KEY,
    MarketNewsIngestTask,
)


class _LLMProbe(LangChainMixin):
    pass


def _probe_gemini(*, model_name: str | None = None) -> Dict[str, Any]:
    probe = _LLMProbe()
    started = time.perf_counter()
    llm = probe._get_gemini_llm(model_name=model_name or None, temperature=0.0, json_mode=False)
    if llm is None:
        return {
            "ok": False,
            "error": "Gemini LLM client could not be initialized. Check GOOGLE_API_KEY and langchain-google-genai.",
        }

    try:
        response = llm.invoke("Reply with exactly: GEMINI_OK")
        content = response.content if hasattr(response, "content") else str(response)
        return {
            "ok": True,
            "latency_seconds": round(time.perf_counter() - started, 3),
            "response_preview": str(content)[:200],
        }
    except Exception as exc:
        return {
            "ok": False,
            "latency_seconds": round(time.perf_counter() - started, 3),
            "error": str(exc),
        }


def _probe_binance(*, websocket_url: str) -> Dict[str, Any]:
    started = time.perf_counter()
    try:
        import websocket
        sslopt: Dict[str, Any] = {}
        ca_file = str(
            os.environ.get("SSL_CERT_FILE")
            or os.environ.get("CA_PATH")
            or ""
        ).strip()
        if ca_file:
            sslopt["ca_certs"] = ca_file

        ws = websocket.create_connection(websocket_url, timeout=10, sslopt=sslopt or None)
        try:
            message = ws.recv()
        finally:
            ws.close()

        parsed: Any
        try:
            parsed = json.loads(message)
        except Exception:
            parsed = message

        return {
            "ok": True,
            "latency_seconds": round(time.perf_counter() - started, 3),
            "websocket_url": websocket_url,
            "message_preview": str(parsed)[:300],
        }
    except Exception as exc:
        return {
            "ok": False,
            "latency_seconds": round(time.perf_counter() - started, 3),
            "websocket_url": websocket_url,
            "error": str(exc),
        }


def _probe_fmp(*, symbol: str) -> Dict[str, Any]:
    started = time.perf_counter()
    if not FMP_API_KEY:
        return {
            "ok": False,
            "error": "FMP_API_KEY is not configured.",
        }

    task = MarketNewsIngestTask()
    candidates = task._candidate_symbols(symbol)
    base_url = (
        "https://financialmodelingprep.com/stable/news/crypto"
        if task._is_crypto_symbol(symbol)
        else "https://financialmodelingprep.com/stable/news/stock"
    )
    attempts: List[Dict[str, Any]] = []
    for candidate in candidates:
        url = f"{base_url}?symbols={candidate}&limit=8&apikey={FMP_API_KEY}"
        try:
            response = requests.get(url, timeout=15)
            attempts.append(
                {
                    "candidate": candidate,
                    "status_code": response.status_code,
                }
            )
            response.raise_for_status()
            payload = response.json()
            normalized = task._normalize_records(payload, symbol=symbol, provider="fmp")
            if normalized:
                return {
                    "ok": True,
                    "latency_seconds": round(time.perf_counter() - started, 3),
                    "symbol": symbol,
                    "resolved_candidate": candidate,
                    "headline_count": len(normalized),
                    "headline_preview": normalized[0]["headline"],
                    "attempts": attempts,
                }
        except Exception as exc:
            attempts.append(
                {
                    "candidate": candidate,
                    "error": str(exc),
                }
            )

    return {
        "ok": False,
        "latency_seconds": round(time.perf_counter() - started, 3),
        "symbol": symbol,
        "attempts": attempts,
        "error": "FMP returned no normalized rows for all symbol candidates.",
    }


def run_provider_smoke_check(
    *,
    symbol: str = DEFAULT_SYMBOL,
    websocket_url: str = DEFAULT_WEBSOCKET_URL,
    gemini_model: str | None = None,
) -> Dict[str, Any]:
    logger.info("[ProviderSmoke] Checking Gemini connectivity")
    gemini = _probe_gemini(model_name=gemini_model)

    logger.info("[ProviderSmoke] Checking Binance websocket connectivity")
    binance = _probe_binance(websocket_url=websocket_url)

    logger.info("[ProviderSmoke] Checking FMP news connectivity")
    fmp = _probe_fmp(symbol=symbol)

    return {
        "symbol": symbol,
        "websocket_url": websocket_url,
        "gemini": gemini,
        "binance": binance,
        "fmp": fmp,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Smoke-check Gemini, Binance, and FMP connectivity from the current runtime.")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--websocket-url", default=DEFAULT_WEBSOCKET_URL)
    parser.add_argument("--gemini-model", default="")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    result = run_provider_smoke_check(
        symbol=str(args.symbol).upper(),
        websocket_url=str(args.websocket_url).strip(),
        gemini_model=str(args.gemini_model).strip() or None,
    )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
