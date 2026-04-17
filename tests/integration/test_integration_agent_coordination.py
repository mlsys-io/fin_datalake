"""
Integration tests for multi-agent coordination via AgentHub.

These tests require a local Ray cluster (started automatically via fixture).
Run with: pytest tests/integration/ -v
"""
import pytest
import ray
import time

pytestmark = pytest.mark.integration

# ============================================================================
# Test Agents (Mock implementations)
# ============================================================================

def create_test_agent(name: str, capabilities: list, response: str):
    """Factory to create test agent classes."""
    from etl.agents.base import BaseAgent
    
    @ray.remote
    class TestAgent(BaseAgent):
        CAPABILITIES = capabilities
        
        def __init__(self):
            super().__init__()
            self._response = response
        
        def build_executor(self):
            return lambda x: f"{self._response}: {x}"
    
    return TestAgent.options(name=name).remote()

# ============================================================================
# Integration Tests: Agent Hub
# ============================================================================

class TestAgentHubIntegration:
    """Integration tests for AgentHub."""
    
    def test_auto_registration_on_setup(self, clean_hub):
        """Test that agents auto-register when setup() is called."""
        from etl.agents.base import BaseAgent
        
        @ray.remote
        class AutoRegisterAgent(BaseAgent):
            CAPABILITIES = ["test_capability"]
            def build_executor(self):
                return lambda x: x
        
        # Deploy and setup
        actor = AutoRegisterAgent.options(name="AutoRegAgent").remote()
        ray.get(actor.set_app_name.remote("AutoRegAgent"))
        ray.get(actor.setup.remote())
        
        # Verify registration
        agents = ray.get(clean_hub.find_by_capability.remote("test_capability"))
        assert "AutoRegAgent" in agents
        
        # Cleanup
        ray.kill(actor)
    
    def test_find_by_capability(self, clean_hub):
        """Test capability-based agent discovery."""
        # Register multiple agents manually (simulating registration)
        ray.get(clean_hub.register.remote("Agent1", ["analysis", "data"]))
        ray.get(clean_hub.register.remote("Agent2", ["analysis"]))
        ray.get(clean_hub.register.remote("Agent3", ["data", "storage"]))
        
        # Find by capability
        analysis_agents = ray.get(clean_hub.find_by_capability.remote("analysis"))
        assert len(analysis_agents) == 2
        assert "Agent1" in analysis_agents
        assert "Agent2" in analysis_agents
        
        storage_agents = ray.get(clean_hub.find_by_capability.remote("storage"))
        assert len(storage_agents) == 1
        assert "Agent3" in storage_agents
    
    def test_stale_agent_detection(self, clean_hub):
        """Test that dead agents are detected and removed."""
        from etl.agents.base import BaseAgent
        
        @ray.remote
        class EphemeralAgent(BaseAgent):
            CAPABILITIES = ["ephemeral"]
            def build_executor(self):
                return lambda x: x
        
        # Create and register
        actor = EphemeralAgent.options(name="EphemeralAgent").remote()
        ray.get(actor.setup.remote())
        
        # Verify registered
        assert "EphemeralAgent" in ray.get(clean_hub.find_by_capability.remote("ephemeral"))
        
        # Kill the actor
        ray.kill(actor)
        time.sleep(0.5)  # Give Ray time to update state
        
        # Hub should throw ValueError when calling a dead agent, and clean it up
        with pytest.raises(ValueError, match="not registered or not alive"):
            ray.get(clean_hub.call.remote("EphemeralAgent", {"data": "test"}))
        
        # And it should no longer be listed globally
        agents = ray.get(clean_hub.list_agents.remote())
        agent_names = [a["name"] for a in agents]
        assert "EphemeralAgent" not in agent_names


# ============================================================================
# Integration Tests: Notifications
# ============================================================================

class TestAgentNotifications:
    """Integration tests for fire-and-forget notifications on the Hub."""
    
    def test_notify_capability_delivers_messages(self, clean_hub):
        """Test that notifications are delivered to specific capabilities."""
        from etl.agents.base import BaseAgent

        class ListenerAgent(BaseAgent):
            CAPABILITIES = ["listener"]

            def build_executor(self):
                return lambda x: x
            
            def handle_event(self, event):
                self.last_message = event

            def get_last_message(self):
                return getattr(self, 'last_message', None)

        listener = ListenerAgent.deploy(name="ListenerAgent")
        try:
            event = {"topic": "test_topic", "payload": {"data": "hello"}}
            delivered = ray.get(clean_hub.notify_capability.remote("listener", event))
            assert delivered == 1

            time.sleep(0.5)

            msg = listener.get_last_message.remote().result()
            assert msg is not None
            assert msg["payload"] == {"data": "hello"}
            assert msg["topic"] == "test_topic"
        finally:
            from ray import serve

            serve.delete("ListenerAgent")
            ray.get(clean_hub.unregister.remote("ListenerAgent"))


# ============================================================================
# Integration Tests: Agent Delegation
# ============================================================================

class TestAgentDelegationIntegration:
    """Integration tests for agent-to-agent synchronous delegation."""
    
    def test_delegate_routes_to_capable_agent(self, clean_hub):
        """Test that delegate() finds and calls the right agent via the Hub."""
        from etl.agents.base import BaseAgent

        class AnalystAgent(BaseAgent):
            CAPABILITIES = ["analysis"]
            def build_executor(self):
                return lambda x: f"Analyzed: {x}"

        class CoordinatorAgent(BaseAgent):
            CAPABILITIES = ["coordination"]
            def build_executor(self):
                return lambda x: x
            
            def run_analysis(self, data):
                from etl.core.base_service import SyncHandle
                from etl.agents.hub import get_hub
                hub = SyncHandle(get_hub())
                return hub.call_by_capability("analysis", data)

        analyst = AnalystAgent.deploy(name="Analyst")
        coordinator = CoordinatorAgent.deploy(name="Coordinator")
        try:
            result = coordinator.run_analysis.remote("test data").result()
            assert result == "Analyzed: test data"
        finally:
            from ray import serve

            serve.delete("Coordinator")
            serve.delete("Analyst")
            ray.get(clean_hub.unregister.remote("Coordinator"))
            ray.get(clean_hub.unregister.remote("Analyst"))
    
    def test_delegate_retry_on_failure(self, clean_hub):
        """Test that capability routing skips dead registry entries and uses an alive deployment."""
        ray.get(clean_hub.register.remote("DeadAgent", ["retry_test"]))
        from etl.agents.base import BaseAgent

        class AliveTestAgent(BaseAgent):
            CAPABILITIES = ["retry_test"]
            def build_executor(self):
                return lambda x: f"Success: {x}"

        alive = AliveTestAgent.deploy(name="AliveAgent")

        class DelegatorAgent(BaseAgent):
            CAPABILITIES = []
            def build_executor(self):
                return lambda x: x
            
            def test_delegation(self, data):
                from etl.core.base_service import SyncHandle
                from etl.agents.hub import get_hub
                hub = SyncHandle(get_hub())
                return hub.call_by_capability("retry_test", data)

        delegator = DelegatorAgent.deploy(name="Delegator")
        try:
            result = delegator.test_delegation.remote("data").result()
            assert result == "Success: data"
        finally:
            from ray import serve

            serve.delete("Delegator")
            serve.delete("AliveAgent")
            ray.get(clean_hub.unregister.remote("Delegator"))
            ray.get(clean_hub.unregister.remote("AliveAgent"))
            ray.get(clean_hub.unregister.remote("DeadAgent"))
