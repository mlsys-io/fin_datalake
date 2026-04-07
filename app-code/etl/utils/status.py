"""
Service Registry & Status CLI
Usage: python -m etl.utils.status [--namespace=<name>]

Connects to the Ray Cluster and lists the status of all known ServiceTasks/Agents.
Enforces the 'suffix strategy': Default namespace is 'zdb_etl_service'.
"""

import argparse
import ray
from ray.util.state import list_actors
from rich.console import Console
from rich.table import Table

DEFAULT_NAMESPACE = "zdb_etl_service"

def get_status(namespace: str):
    """
    Connects to Ray and fetches actor statuses for the given namespace.
    """
    try:
        # Try connecting to existing cluster
        ray.init(address="auto", namespace=namespace, ignore_reinit_error=True)
    except ConnectionError:
        print("❌ Could not connect to Ray Cluster. Is 'ray start --head' running?")
        return

    print(f"📡 Connected to Ray Cluster (Namespace: {namespace})")
    
    # Fetch all actors
    # logical_actor_ids=True gives us the friendly 'name' we assigned in ServiceTask
    actors = list_actors(filters=[("state", "=", "ALIVE")])
    
    # Filter for our components? 
    # For now, we show all ALIVE actors in this namespace as they are likely relevant.
    
    if not actors:
        print("Build 'zdb_etl_service' namespace is empty. No services running.")
        return

    console = Console()
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Service Name", style="dim", width=30)
    table.add_column("State", style="green")
    table.add_column("Actor ID", style="cyan")
    table.add_column("Node IP")

    for actor in actors:
        name = actor.get("name", "Unknown")
        state = actor.get("state", "UNKNOWN")
        actor_id = actor.get("actor_id", "-")
        ip = actor.get("address", {}).get("ip_address", "-")
        
        table.add_row(name, state, actor_id, ip)

    console.print(table)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check ETL Service Status")
    parser.add_argument("--namespace", type=str, default=DEFAULT_NAMESPACE, help="Ray Namespace to inspect")
    args = parser.parse_args()
    
    get_status(args.namespace)