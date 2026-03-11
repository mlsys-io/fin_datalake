"""
AgentHub — Central coordination point for all agent operations.

Consolidates service discovery (formerly AgentRegistry) and communication
(formerly MessageBus) into one component. Deployed as a singleton Ray Actor.

Features:
- Agent registration and capability-based discovery
- Synchronous call routing (call, call_by_capability)
- Fire-and-forget notifications (notify, notify_all, notify_capability)
- Health monitoring and lifecycle management
- Auto-cleanup of dead agents

Design:
- Lives INSIDE Ray as a detached Actor for fast agent-to-agent access
- External users access via Gateway (AgentAdapter → AgentHub)
- Agents self-register on startup → no persistence needed
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
    Central coordination point for multi-agent systems.

    Handles discovery, synchronous routing, notifications, and health
    monitoring — all in one component.

    Usage via SyncHandle::

        hub = SyncHandle(get_hub())

        # Discovery
        hub.register("AnalystAgent", capabilities=["analysis"])
        agents = hub.find_by_capability("analysis")

        # Synchronous calls (returns result)
        result = hub.call("AnalystAgent", payload)
        result = hub.call_by_capability("analysis", payload)

        # Fire-and-forget notifications
        hub.notify("AnalystAgent", event)
        hub.notify_capability("analysis", event)
    """

    def __init__(self):
        from etl.utils.logging import setup_logging
        from loguru import logger

        setup_logging(component="hub")

        self._agents: Dict[str, AgentInfo] = {}
        self._capability_index: Dict[str, List[str]] = defaultdict(list)
        self._stats = {
            "total_calls": 0,
            "total_notifications": 0,
            "failed_calls": 0,
        }
        logger.info("AgentHub initialized")

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
    # Synchronous Routing
    # =========================================================================

    def call(self, name: str, payload: Any) -> Any:
        """
        Synchronous call to a specific agent. Calls ask() and returns the result.

        Args:
            name: Target agent name
            payload: Input to pass to the agent's ask() method

        Returns:
            Result from the agent

        Raises:
            ValueError: If agent not found or not alive
        """
        from loguru import logger

        self._stats["total_calls"] += 1

        handle = self._get_handle(name)
        if handle is None:
            self._stats["failed_calls"] += 1
            raise ValueError(f"Agent '{name}' is not registered or not alive.")

        try:
            return ray.get(handle.ask.remote(payload))
        except Exception as e:
            self._stats["failed_calls"] += 1
            logger.error(f"Call to '{name}' failed: {e}")
            raise

    def call_by_capability(
        self,
        capability: str,
        payload: Any,
        retry_on_failure: bool = True,
        max_retries: int = 3,
    ) -> Any:
        """
        Route a call to the best available agent with a given capability.

        Tries agents in registration order. If retry_on_failure is True,
        tries the next agent if the first one fails.

        Args:
            capability: Required capability
            payload: Input to the agent
            retry_on_failure: Try other agents on failure (default: True)
            max_retries: Max agents to try (default: 3)

        Returns:
            Result from the first successful agent

        Raises:
            ValueError: If no agent with capability exists
            RuntimeError: If all capable agents fail
        """
        from loguru import logger

        agents = self.find_by_capability(capability)
        if not agents:
            raise ValueError(f"No agent with capability '{capability}'")

        self._stats["total_calls"] += 1
        attempts = min(len(agents), max_retries) if retry_on_failure else 1
        last_error = None

        for i in range(attempts):
            target_name = agents[i]
            try:
                handle = self._get_handle(target_name)
                if handle is None:
                    logger.warning(f"Agent '{target_name}' not accessible, trying next...")
                    continue

                logger.info(f"Routing '{capability}' to '{target_name}'")
                return ray.get(handle.ask.remote(payload))
            except Exception as e:
                last_error = e
                logger.warning(f"Agent '{target_name}' failed: {e}")
                if not retry_on_failure:
                    raise

        self._stats["failed_calls"] += 1
        raise RuntimeError(
            f"All agents with capability '{capability}' failed. "
            f"Last error: {last_error}"
        )

    # =========================================================================
    # Notifications (fire-and-forget)
    # =========================================================================

    def notify(self, name: str, event: Dict[str, Any]) -> bool:
        """
        Fire-and-forget notification to a specific agent.

        Args:
            name: Target agent name
            event: Event dict with topic, payload, sender, etc.

        Returns:
            True if dispatched, False if agent not found
        """
        handle = self._get_handle(name)
        if handle is None:
            return False

        handle.notify.remote(event)
        self._stats["total_notifications"] += 1
        return True

    def notify_all(self, event: Dict[str, Any]) -> int:
        """
        Broadcast a notification to ALL registered agents.

        Args:
            event: Event dict

        Returns:
            Number of agents notified
        """
        delivered = 0
        for name in list(self._agents.keys()):
            if self.notify(name, event):
                delivered += 1
        return delivered

    def notify_capability(
        self, capability: str, event: Dict[str, Any]
    ) -> int:
        """
        Notify all agents with a specific capability.

        Args:
            capability: Target capability
            event: Event dict

        Returns:
            Number of agents notified
        """
        agents = self.find_by_capability(capability)
        delivered = 0
        for name in agents:
            if self.notify(name, event):
                delivered += 1
        return delivered

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
        """
        Get a fresh Ray actor handle for an agent.

        Returns None if agent is not registered or not alive.
        Always fetches from Ray to avoid stale references.
        """
        if name not in self._agents:
            return None
        try:
            return ray.get_actor(name)
        except ValueError:
            from loguru import logger

            logger.warning(f"Agent '{name}' is dead, removing from hub")
            self.unregister(name)
            return None

    def _is_alive(self, name: str) -> bool:
        """Check if a named Ray actor is still alive (cheap namespace lookup)."""
        try:
            ray.get_actor(name)
            return True
        except ValueError:
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
