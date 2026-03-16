"""
StatusReporterActuator — Generates a live Markdown report of the system status.
"""

from __future__ import annotations

import datetime
import os
from loguru import logger

from overseer.actuators.base import BaseActuator
from overseer.models import ActionResult, OverseerAction, SystemSnapshot
from overseer.store import MetricsStore


class StatusReporterActuator(BaseActuator):
    """
    Actuator that generates a 'living documentation' artifact.
    It reads the latest snapshot and recent alerts from the MetricsStore
    and writes them to docs/system_status.md.
    """

    def __init__(self, store: MetricsStore, output_path: str = "docs/system_status.md"):
        self.store = store
        self.output_path = output_path

    async def execute(self, action: OverseerAction) -> ActionResult:
        """
        Actually triggers the report generation.
        The 'action' contains any special reason/message to include.
        """
        try:
            # 1. Fetch latest state
            snapshot = await self.store.latest()
            alerts = await self.store.recent_alerts(n=5)
            
            if not snapshot:
                return ActionResult(success=False, error="No snapshot found in store.")

            # 2. Build the Markdown
            md = self._build_markdown(snapshot, alerts, action.reason)

            # 3. Write to file
            # Ensure the directory exists (relative to project root if execution is from root)
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            with open(self.output_path, "w", encoding="utf-8") as f:
                f.write(md)

            return ActionResult(success=True, detail=f"System status updated at {self.output_path}")

        except Exception as e:
            logger.error(f"StatusReporterActuator failed: {e}")
            return ActionResult(success=False, error=str(e))

    def _build_markdown(self, snapshot: SystemSnapshot, alerts: list[dict], reason: str) -> str:
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        sections = [
            "# 📡 AI Lakehouse System Status",
            f"*Last Updated: {now}*",
            f"\n> [!NOTE]\n> {reason or 'Continuous Autonomic Monitoring Active'}\n",
        ]

        # Table: Service Health
        sections.append("## 🏥 Service Health")
        sections.append("| Service | Health | Last Seen | Error/Latency |")
        sections.append("|:--- |:--- |:--- |:--- |")
        
        for name, m in snapshot.services.items():
            status = "🟢 Healthy" if m.healthy else "🔴 DEGRADED"
            last_seen = datetime.datetime.fromtimestamp(m.collected_at).strftime("%H:%M:%S")
            err = m.error or "---"
            sections.append(f"| **{name}** | {status} | {last_seen} | {err} |")

        # Table: Recent Autonomic Actions
        sections.append("\n## 🧠 Recent Autonomic Actions (MAPE-K Loop)")
        if not alerts:
             sections.append("*No recent actions taken (System stable).*")
        else:
            sections.append("| Timestamp | Action | Target | Reason |")
            sections.append("|:--- |:--- |:--- |:--- |")
            for a in alerts:
                ts = datetime.datetime.fromtimestamp(a.get("timestamp", 0)).strftime("%H:%M:%S")
                typ = a.get("type", "alert").upper()
                target = a.get("target", "ray")
                reason = a.get("reason", "---")
                sections.append(f"| {ts} | `{typ}` | {target} | {reason} |")

        sections.append("\n---")
        sections.append("*Generated automatically by the Overseer System.*")
        
        return "\n".join(sections)
