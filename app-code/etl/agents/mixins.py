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
        ctx = self._get_context_store()
        key = f"thread:{conversation_id}"
        # Use self.name if available, else "Unknown"
        owner = getattr(self, "name", "UnknownAgent")
        ctx.set(key, messages, ttl=ttl, owner=owner)

    def load_conversation_state(self, conversation_id: str) -> List[Dict[str, Any]]:
        """
        Load the full conversation history.
        
        Args:
            conversation_id: Unique ID for the thread
            
        Returns:
            List of message dicts (empty if not found)
        """
        ctx = self._get_context_store()
        key = f"thread:{conversation_id}"
        data = ctx.get(key)
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
        ctx.append(key, msg, ttl=ttl, owner=owner)

    def trim_history(
        self, 
        messages: List[Dict[str, Any]], 
        max_messages: int = 20, 
        max_tokens: int = 4000
    ) -> List[Dict[str, Any]]:
        """
        Trim conversation history to stay within limits.
        
        Deterministic algorithm:
        1. Keep at most max_messages.
        2. Keep total estimated tokens under max_tokens.
        3. Always preserve the 'system' message if it's the first one.
        """
        if not messages:
            return []
            
        # 1. Cap by message count
        system_msg = None
        if messages[0].get("role") == "system":
            system_msg = messages[0]
            work_list = messages[1:]
        else:
            work_list = messages
            
        if len(work_list) > max_messages:
            work_list = work_list[-max_messages:]
            
        # 2. Cap by token count (approximate)
        trimmed = []
        current_tokens = 0
        if system_msg:
            current_tokens += self._estimate_tokens(system_msg.get("content", ""))
            
        # Add messages from newest to oldest until limit reached
        for msg in reversed(work_list):
            tokens = self._estimate_tokens(msg.get("content", ""))
            if current_tokens + tokens > max_tokens:
                break
            trimmed.insert(0, msg)
            current_tokens += tokens
            
        if system_msg:
            trimmed.insert(0, system_msg)
            
        return trimmed

    def _estimate_tokens(self, text: str) -> int:
        """Simple heuristic for token estimation (chars / 4)."""
        if not text:
            return 0
        return len(text) // 4

