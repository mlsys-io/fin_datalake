"""
Message Bus for async pub/sub agent communication.
Push-based delivery - messages are sent directly to agents.
"""
from typing import Dict, List, Any
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict
import ray
from loguru import logger


@dataclass
class Message:
    """Wrapper for messages sent through the bus."""
    topic: str
    payload: Any
    sender: str
    timestamp: datetime = field(default_factory=datetime.utcnow)


@ray.remote
class MessageBus:
    """
    Push-based pub/sub message bus for agent communication.
    
    Messages are delivered immediately to subscriber agents
    via their notify() method (fire-and-forget).
    
    Usage:
        bus = MessageBus.remote()
        
        # Subscribe
        ray.get(bus.subscribe.remote("market_data", "AnalystAgent"))
        
        # Publish - instantly pushed to all subscribers
        ray.get(bus.publish.remote("market_data", {"symbol": "AAPL"}, "DataAgent"))
    """
    
    def __init__(self):
        self._subscriptions: Dict[str, List[str]] = defaultdict(list)
        self._failed_deliveries: Dict[str, int] = defaultdict(int)
        logger.info("MessageBus initialized (push-based)")
    
    def subscribe(self, topic: str, agent_name: str) -> bool:
        """
        Subscribe an agent to a topic.
        
        Args:
            topic: Topic name to subscribe to
            agent_name: Name of the subscribing agent
            
        Returns:
            True if subscribed successfully
        """
        if agent_name not in self._subscriptions[topic]:
            self._subscriptions[topic].append(agent_name)
            logger.info(f"Agent '{agent_name}' subscribed to topic '{topic}'")
        return True
    
    def unsubscribe(self, topic: str, agent_name: str) -> bool:
        """Unsubscribe an agent from a topic."""
        if agent_name in self._subscriptions[topic]:
            self._subscriptions[topic].remove(agent_name)
            return True
        return False
    
    def publish(self, topic: str, payload: Any, sender: str) -> int:
        """
        Publish a message to a topic (push-based).
        
        Immediately delivers to all subscribers via their notify() method.
        
        Args:
            topic: Topic to publish to
            payload: Message content
            sender: Name of the sending agent
            
        Returns:
            Number of agents message was pushed to
        """
        subscribers = self._subscriptions.get(topic, [])
        delivered = 0
        
        message = {
            "topic": topic,
            "payload": payload,
            "sender": sender,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        for agent_name in subscribers:
            try:
                agent = ray.get_actor(agent_name)
                # Fire-and-forget delivery via notify()
                agent.notify.remote(message)
                delivered += 1
            except ValueError:
                # Agent not found
                self._failed_deliveries[agent_name] += 1
                logger.warning(f"Cannot deliver to '{agent_name}': agent not found")
            except Exception as e:
                self._failed_deliveries[agent_name] += 1
                logger.warning(f"Failed to deliver to '{agent_name}': {e}")
        
        logger.debug(f"Published to '{topic}': {delivered}/{len(subscribers)} delivered")
        return delivered
    
    def broadcast(self, payload: Any, sender: str) -> int:
        """
        Broadcast a message to ALL subscribed agents (any topic).
        
        Args:
            payload: Message content
            sender: Name of the sending agent
            
        Returns:
            Number of unique agents message was pushed to
        """
        all_agents = set()
        for agents in self._subscriptions.values():
            all_agents.update(agents)
        
        delivered = 0
        message = {
            "topic": "_broadcast",
            "payload": payload,
            "sender": sender,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        for agent_name in all_agents:
            try:
                agent = ray.get_actor(agent_name)
                agent.notify.remote(message)
                delivered += 1
            except Exception:
                pass
        
        return delivered
    
    def get_topics(self) -> List[str]:
        """Get all topics with subscribers."""
        return list(self._subscriptions.keys())
    
    def get_subscribers(self, topic: str) -> List[str]:
        """Get all subscribers for a topic."""
        return self._subscriptions.get(topic, [])
    
    def get_stats(self) -> Dict:
        """Get delivery statistics."""
        return {
            "topics": len(self._subscriptions),
            "total_subscribers": sum(len(s) for s in self._subscriptions.values()),
            "failed_deliveries": dict(self._failed_deliveries)
        }


def get_bus() -> ray.actor.ActorHandle:
    """
    Get or create the singleton MessageBus actor.
    
    Returns:
        Ray ActorHandle to the MessageBus
    """
    try:
        return ray.get_actor("MessageBus")
    except ValueError:
        return MessageBus.options(name="MessageBus", lifetime="detached").remote()

