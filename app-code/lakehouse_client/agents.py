"""
Client-side helpers for managing the Ray Serve agent fleet.
"""

from etl.agents.manager import (
    baseline_fleet_specs,
    delete_agent,
    delete_baseline_fleet,
    deploy_agent,
    deploy_baseline_fleet,
    list_fleet_state,
)

__all__ = [
    "baseline_fleet_specs",
    "delete_agent",
    "delete_baseline_fleet",
    "deploy_agent",
    "deploy_baseline_fleet",
    "list_fleet_state",
]
