"""
Agent Registry for service discovery and capability-based agent lookup.
Central coordination point for multi-agent systems.
"""
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
import ray
from loguru import logger


@dataclass
class AgentInfo:
    """Metadata about a registered agent."""
    name: str
    capabilities: List[str]
    actor_handle: Optional[Any] = None
    registered_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)


@ray.remote
class AgentRegistry:
    """
    Central service discovery for agents.
    
    Manages agent registration, discovery, and capability-based lookups.
    Deployed as a singleton Ray Actor.
    
    Usage:
        # Start registry
        registry = AgentRegistry.remote()
        
        # Register agent
        ray.get(registry.register.remote("AnalystAgent", ["analysis", "data"]))
        
        # Find agents by capability
        agents = ray.get(registry.find_by_capability.remote("analysis"))
    """
    
    def __init__(self):
        self._agents: Dict[str, AgentInfo] = {}
        self._capability_index: Dict[str, List[str]] = {}
        logger.info("AgentRegistry initialized")
    
    def register(self, name: str, capabilities: List[str], metadata: Dict = None) -> bool:
        """
        Register an agent with its capabilities.
        
        Args:
            name: Unique agent name
            capabilities: List of capabilities this agent provides
            metadata: Optional additional metadata
        
        Returns:
            True if registered successfully
        """
        if name in self._agents:
            logger.warning(f"Agent '{name}' already registered, updating...")
        
        # Try to get actor handle from Ray
        try:
            actor_handle = ray.get_actor(name)
        except ValueError:
            actor_handle = None
            logger.debug(f"No actor found for '{name}', registering without handle")
        
        self._agents[name] = AgentInfo(
            name=name,
            capabilities=capabilities,
            actor_handle=actor_handle,
            metadata=metadata or {}
        )
        
        # Update capability index
        for cap in capabilities:
            if cap not in self._capability_index:
                self._capability_index[cap] = []
            if name not in self._capability_index[cap]:
                self._capability_index[cap].append(name)
        
        logger.info(f"Registered agent '{name}' with capabilities: {capabilities}")
        return True
    
    def unregister(self, name: str) -> bool:
        """Remove an agent from the registry."""
        if name not in self._agents:
            return False
        
        agent = self._agents[name]
        
        # Remove from capability index
        for cap in agent.capabilities:
            if cap in self._capability_index:
                self._capability_index[cap] = [
                    n for n in self._capability_index[cap] if n != name
                ]
        
        del self._agents[name]
        logger.info(f"Unregistered agent '{name}'")
        return True
    
    def find_by_capability(self, capability: str) -> List[str]:
        """
        Find all agents that provide a specific capability.
        
        Args:
            capability: The capability to search for
            
        Returns:
            List of agent names that have this capability
        """
        return self._capability_index.get(capability, [])
    
    def get_agent(self, name: str) -> Optional[Any]:
        """
        Get the Ray actor handle for a registered agent.
        Validates the actor is still alive before returning.
        
        Args:
            name: Agent name
            
        Returns:
            Ray ActorHandle or None if not found or dead
        """
        if name not in self._agents:
            return None
        
        agent = self._agents[name]
        
        # Check if actor is still alive
        if not self._is_actor_alive(name):
            logger.warning(f"Agent '{name}' is no longer alive, removing from registry")
            self.unregister(name)
            return None
        
        # Refresh handle if needed
        if agent.actor_handle is None:
            try:
                agent.actor_handle = ray.get_actor(name)
            except ValueError:
                return None
        
        return agent.actor_handle
    
    def _is_actor_alive(self, name: str) -> bool:
        """
        Check if an actor is still alive using Ray state API.
        
        Args:
            name: Actor name to check
            
        Returns:
            True if actor is ALIVE, False otherwise
        """
        try:
            from ray.util.state import get_actor
            actor_state = get_actor(id=name)
            if actor_state is None:
                # Try getting by name via list_actors
                from ray.util.state import list_actors
                actors = list_actors(filters=[("name", "=", name), ("state", "=", "ALIVE")])
                return len(actors) > 0
            return actor_state.state == "ALIVE"
        except Exception:
            # Fallback: try to get actor handle
            try:
                ray.get_actor(name)
                return True
            except ValueError:
                return False
    
    def list_agents(self) -> List[Dict]:
        """List all registered agents with their info."""
        return [
            {
                "name": info.name,
                "capabilities": info.capabilities,
                "registered_at": info.registered_at.isoformat(),
                "metadata": info.metadata,
                "has_handle": info.actor_handle is not None
            }
            for info in self._agents.values()
        ]
    
    def get_capabilities(self) -> List[str]:
        """Get all known capabilities across all agents."""
        return list(self._capability_index.keys())


def get_registry() -> ray.actor.ActorHandle:
    """
    Get or create the singleton AgentRegistry actor.
    
    Returns:
        Ray ActorHandle to the AgentRegistry
    """
    try:
        return ray.get_actor("AgentRegistry")
    except ValueError:
        return AgentRegistry.options(name="AgentRegistry", lifetime="detached").remote()
