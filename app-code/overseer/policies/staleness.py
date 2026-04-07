"""
DataStalenessPolicy — Emits an ALERT if Delta Lake data isn't being updated.
"""
import time
from overseer.models import ActionType, OverseerAction, SystemSnapshot
from overseer.policies.base import BasePolicy

class DataStalenessPolicy(BasePolicy):
    
    def __init__(self, staleness_threshold_seconds: int = 300, target_service: str = "delta_lake"):
        self.staleness_threshold_seconds = staleness_threshold_seconds
        self.target_service = target_service

    def evaluate(self, snapshot: SystemSnapshot) -> list[OverseerAction]:
        actions = []
        metrics = snapshot.services.get(self.target_service)
        
        if not metrics or not metrics.healthy:
            return actions
            
        tables = metrics.data.get("tables", {})
        current_time = time.time()
        
        for table_name, table_metrics in tables.items():
            if "error" in table_metrics:
                # If a specific table failed, we might want to alert or ignore. We ignore for now.
                continue
                
            latest_time = table_metrics.get("latest_commit_time")
            if not latest_time:
                continue
                
            staleness = current_time - latest_time
            
            if staleness > self.staleness_threshold_seconds:
                actions.append(OverseerAction(
                    type=ActionType.ALERT,
                    target="webhook",
                    agent=f"{self.target_service}/{table_name}",
                    count=1,
                    reason=f"Data staleness detected on {table_name}: Last write was {staleness:.1f} seconds ago (Threshold is {self.staleness_threshold_seconds}s).",
                ))
            
        return actions
