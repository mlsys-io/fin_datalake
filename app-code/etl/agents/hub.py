"""
AgentHub — Central service discovery registry for all agents.

Consolidates agent registration and capability-based discovery into one detached singleton.
Routing is now handled by Ray Serve directly.

Features:
- Agent registration and capability-based discovery
- Health monitoring and stats of the agent ecosystem
- Auto-cleanup of stale registration records
"""
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict
import ray


@dataclass
class AgentInfo:
    """Metadata about a registered agent."""

    name: str
    capabilities: List[str]
    registered_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)


@ray.remote
class AgentHub:
    """
    Central discovery registry for the multi-agent system.

    Handles registration and capability-based lookups.
    Interaction is now handled via Ray Serve handles or HTTP.

    Usage via SyncHandle::

        hub = SyncHandle(get_hub())

        # Discovery
        hub.register("AnalystAgent", capabilities=["analysis"])
        agents = hub.find_by_capability("analysis")
    """

    def __init__(self):
        from etl.utils.logging import setup_logging
        from loguru import logger

        setup_logging(component="hub")

        self._agents: Dict[str, AgentInfo] = {}
        self._capability_index: Dict[str, List[str]] = defaultdict(list)
        self._stats = {
            "total_registrations": 0,
        }
        logger.info("AgentHub (Pure Registry) initialized")

    # =========================================================================
    # Discovery
    # =========================================================================

    def register(
        self,
        name: str,
        capabilities: List[str],
        metadata: Dict = None,
    ) -> bool:
        """
        Register an agent with its capabilities.

        Called automatically by BaseAgent.setup() when CAPABILITIES are defined.

        Args:
            name: Unique agent name (must match the Ray actor name)
            capabilities: List of capabilities this agent provides
            metadata: Optional additional metadata

        Returns:
            True if registered successfully
        """
        from loguru import logger

        if name in self._agents:
            # Re-registration: clean up old capability index entries
            self._remove_from_capability_index(name)
            logger.info(f"Agent '{name}' re-registering (updating capabilities)")

        self._agents[name] = AgentInfo(
            name=name,
            capabilities=capabilities,
            metadata=metadata or {},
        )

        # Update capability index
        for cap in capabilities:
            if name not in self._capability_index[cap]:
                self._capability_index[cap].append(name)

        logger.info(f"Registered '{name}' with capabilities: {capabilities}")
        self._stats["total_registrations"] += 1
        return True

    def unregister(self, name: str) -> bool:
        """
        Remove an agent from the hub.

        Cleans up capability index. Called by BaseAgent.on_stop() during shutdown.

        Args:
            name: Agent name to remove

        Returns:
            True if found and removed, False if not found
        """
        if name not in self._agents:
            return False

        self._remove_from_capability_index(name)
        del self._agents[name]

        from loguru import logger

        logger.info(f"Unregistered '{name}'")
        return True

    def _remove_from_capability_index(self, name: str):
        """Remove an agent name from all capability index entries."""
        for cap, agents in self._capability_index.items():
            if name in agents:
                agents.remove(name)

    def find_by_capability(self, capability: str) -> List[str]:
        """
        Find all agents that provide a specific capability.

        Args:
            capability: The capability to search for

        Returns:
            List of agent names that have this capability
        """
        return list(self._capability_index.get(capability, []))

    def list_agents(self) -> List[Dict]:
        """List all registered agents with their metadata."""
        return [
            {
                "name": info.name,
                "capabilities": info.capabilities,
                "registered_at": info.registered_at.isoformat(),
                "metadata": info.metadata,
                "alive": self._is_alive(info.name),
            }
            for info in self._agents.values()
        ]

    def get_capabilities(self) -> List[str]:
        """Get all known capabilities across all agents."""
        return [
            cap
            for cap, agents in self._capability_index.items()
            if agents  # Only return capabilities that have at least one agent
        ]

    # =========================================================================
    # Routing is now handled by Ray Serve directly.
    # Legacy methods call()/notify() are removed.
    # =========================================================================

    # =========================================================================
    # Lifecycle & Health
    # =========================================================================

    def is_alive(self, name: str) -> bool:
        """Check if an agent is alive."""
        return self._is_alive(name)

    def health_check(self) -> Dict[str, bool]:
        """
        Check health of all registered agents.

        Returns:
            Dict mapping agent name → alive status
        """
        return {name: self._is_alive(name) for name in self._agents}

    def get_stats(self) -> Dict:
        """Get hub statistics."""
        return {
            "registered_agents": len(self._agents),
            "capabilities": len(
                [c for c, a in self._capability_index.items() if a]
            ),
            **self._stats,
        }

    # =========================================================================
    # Internal Helpers
    # =========================================================================

    def _get_handle(self, name: str):
        """Get the ServeHandle for an agent."""
        import ray.serve as serve
        try:
            return serve.get_app_handle(name)
        except Exception:
            self.unregister(name)
            return None

    def _is_alive(self, name: str) -> bool:
        """Check if a Ray Serve app is alive."""
        import ray.serve as serve
        try:
            # serve.get_app_handle raises if app doesn't exist
            serve.get_app_handle(name)
            return True
        except (ValueError, KeyError, RuntimeError):
            return False


def get_hub() -> ray.actor.ActorHandle:
    """
    Get or create the singleton AgentHub actor.

    Thread-safe: handles race condition where two processes
    try to create the hub simultaneously.

    Returns:
        Ray ActorHandle to the AgentHub
    """
    try:
        return ray.get_actor("AgentHub")
    except ValueError:
        try:
            return AgentHub.options(
                name="AgentHub", lifetime="detached"
            ).remote()
        except ValueError:
            # Another process created it between our check and create
            return ray.get_actor("AgentHub")
