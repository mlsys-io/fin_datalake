from __future__ import annotations

from typing import Any, Dict, List

import ray

from etl.agents.base import BaseAgent


class BenchmarkTableAgent(BaseAgent):
    """
    Minimal benchmark-only agent used to measure transport overhead.

    The agent intentionally performs only a tiny amount of work so the
    benchmark stays focused on serialized vs zero-copy data handoff.
    """

    def build_executor(self):
        def _execute(payload: Dict[str, Any]) -> Dict[str, Any]:
            rows = self._resolve_rows(payload)
            row_count = len(rows)

            first_timestamp = rows[0].get("timestamp") if row_count else None
            last_close = rows[-1].get("close") if row_count else None

            return {
                "symbol": str(payload.get("symbol", "UNKNOWN")).upper(),
                "row_count": row_count,
                "first_timestamp": first_timestamp,
                "last_close": last_close,
            }

        return _execute

    def _resolve_rows(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        if "data_ref" in payload:
            table = ray.get(payload["data_ref"])
            return table.to_pylist()
        return list(payload.get("ohlc_data") or [])

