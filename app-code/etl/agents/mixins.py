"""
Mixins for Agent capabilities.
"""
from typing import List, Dict, Any, Optional

class ConversationManagerMixin:
    """
    Mixin to provide Conversation State Management capabilities to an Agent.
    Assumes the host class has a `name` attribute.
    """
    
    def _get_context_store(self):
        """Helper to get the ContextStore actor."""
        from etl.agents.context import get_context
        return get_context()

    def save_conversation_state(self, conversation_id: str, messages: list, ttl: int = 3600):
        """
        Save/Overwrite the full conversation history.
        
        Args:
            conversation_id: Unique ID for the thread
            messages: List of message dicts
            ttl: Time-to-live in seconds (default 1h)
        """
        import ray
        ctx = self._get_context_store()
        key = f"thread:{conversation_id}"
        # Use self.name if available, else "Unknown"
        owner = getattr(self, "name", "UnknownAgent")
        ray.get(ctx.set.remote(key, messages, ttl=ttl, owner=owner))

    def load_conversation_state(self, conversation_id: str) -> List[Dict[str, Any]]:
        """
        Load the full conversation history.
        
        Args:
            conversation_id: Unique ID for the thread
            
        Returns:
            List of message dicts (empty if not found)
        """
        import ray
        ctx = self._get_context_store()
        key = f"thread:{conversation_id}"
        data = ray.get(ctx.get.remote(key))
        return data if isinstance(data, list) else []

    def append_message(
        self, 
        conversation_id: str, 
        role: str, 
        content: str, 
        ttl: int = 3600,
        metadata: Optional[Dict] = None
    ):
        """
        Append a single message to the conversation history.
        
        Args:
            conversation_id: Unique ID for the thread
            role: 'user', 'assistant', 'system'
            content: The message text
            ttl: Refresh TTL (default 1h)
        """
        import ray
        from datetime import datetime
        
        ctx = self._get_context_store()
        key = f"thread:{conversation_id}"
        owner = getattr(self, "name", "UnknownAgent")
        
        msg = {
            "role": role,
            "content": content,
            "timestamp": datetime.utcnow().isoformat(),
            "sender": owner
        }
        
        if metadata:
            msg["metadata"] = metadata
        
        # Use the atomic append method in ContextStore
        ray.get(ctx.append.remote(key, msg, ttl=ttl, owner=owner))
