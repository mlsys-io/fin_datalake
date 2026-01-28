"""
Integration tests for multi-agent coordination.

These tests require a local Ray cluster (started automatically via fixture).
Run with: pytest tests/integration/ -v

Naming convention: test_integration_*.py for integration tests
"""
import pytest
import ray
import time


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture(scope="module")
def ray_cluster():
    """Initialize a local Ray cluster for integration testing."""
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True, num_cpus=4)
    yield
    # Don't shutdown - let other tests reuse


@pytest.fixture
def clean_registry(ray_cluster):
    """Ensure clean registry state for each test."""
    from etl.agents.registry import get_registry
    
    registry = get_registry()
    # Clear any existing agents
    agents = ray.get(registry.list_agents.remote())
    for agent_info in agents:
        ray.get(registry.unregister.remote(agent_info["name"]))
    
    yield registry


@pytest.fixture
def clean_bus(ray_cluster):
    """Get a clean message bus instance."""
    from etl.agents.bus import get_bus
    return get_bus()


# ============================================================================
# Test Agents (Mock implementations)
# ============================================================================

def create_test_agent(name: str, capabilities: list, response: str):
    """Factory to create test agent classes."""
    from etl.agents import BaseAgent
    
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
# Integration Tests: Agent Registry
# ============================================================================

class TestAgentRegistryIntegration:
    """Integration tests for AgentRegistry."""
    
    def test_auto_registration_on_setup(self, clean_registry):
        """Test that agents auto-register when setup() is called."""
        from etl.agents import BaseAgent
        
        @ray.remote
        class AutoRegisterAgent(BaseAgent):
            CAPABILITIES = ["test_capability"]
            def build_executor(self):
                return lambda x: x
        
        # Deploy and setup
        actor = AutoRegisterAgent.options(name="AutoRegAgent").remote()
        ray.get(actor.setup.remote())
        
        # Verify registration
        agents = ray.get(clean_registry.find_by_capability.remote("test_capability"))
        assert "AutoRegAgent" in agents
        
        # Cleanup
        ray.kill(actor)
    
    def test_find_by_capability(self, clean_registry):
        """Test capability-based agent discovery."""
        # Register multiple agents with overlapping capabilities
        ray.get(clean_registry.register.remote("Agent1", ["analysis", "data"]))
        ray.get(clean_registry.register.remote("Agent2", ["analysis"]))
        ray.get(clean_registry.register.remote("Agent3", ["data", "storage"]))
        
        # Find by capability
        analysis_agents = ray.get(clean_registry.find_by_capability.remote("analysis"))
        assert len(analysis_agents) == 2
        assert "Agent1" in analysis_agents
        assert "Agent2" in analysis_agents
        
        storage_agents = ray.get(clean_registry.find_by_capability.remote("storage"))
        assert len(storage_agents) == 1
        assert "Agent3" in storage_agents
    
    def test_stale_agent_detection(self, clean_registry):
        """Test that dead agents are detected and removed."""
        from etl.agents import BaseAgent
        
        @ray.remote
        class EphemeralAgent(BaseAgent):
            CAPABILITIES = ["ephemeral"]
            def build_executor(self):
                return lambda x: x
        
        # Create and register
        actor = EphemeralAgent.options(name="EphemeralAgent").remote()
        ray.get(actor.setup.remote())
        
        # Verify registered
        agents = ray.get(clean_registry.find_by_capability.remote("ephemeral"))
        assert "EphemeralAgent" in agents
        
        # Kill the actor
        ray.kill(actor)
        time.sleep(0.5)  # Give Ray time to update state
        
        # get_agent should detect dead actor and return None
        handle = ray.get(clean_registry.get_agent.remote("EphemeralAgent"))
        assert handle is None


# ============================================================================
# Integration Tests: Message Bus
# ============================================================================

class TestMessageBusIntegration:
    """Integration tests for push-based MessageBus."""
    
    def test_publish_delivers_to_subscriber(self, ray_cluster, clean_bus):
        """Test that published messages are delivered to subscribers."""
        from etl.agents import BaseAgent
        
        received_messages = []
        
        @ray.remote
        class ListenerAgent(BaseAgent):
            CAPABILITIES = []
            
            def build_executor(self):
                return lambda x: x
            
            def on_message(self, event):
                # Store received message
                self.last_message = event
            
            def get_last_message(self):
                return getattr(self, 'last_message', None)
        
        # Deploy listener
        listener = ListenerAgent.options(name="ListenerAgent").remote()
        ray.get(listener.setup.remote())
        
        # Subscribe to topic
        ray.get(clean_bus.subscribe.remote("test_topic", "ListenerAgent"))
        
        # Publish message
        ray.get(clean_bus.publish.remote("test_topic", {"data": "hello"}, "Sender"))
        
        # Give time for async delivery
        time.sleep(0.5)
        
        # Verify receipt
        msg = ray.get(listener.get_last_message.remote())
        assert msg is not None
        assert msg["payload"] == {"data": "hello"}
        assert msg["topic"] == "test_topic"
        
        # Cleanup
        ray.kill(listener)


# ============================================================================
# Integration Tests: Agent Delegation
# ============================================================================

class TestAgentDelegationIntegration:
    """Integration tests for agent-to-agent delegation."""
    
    def test_delegate_routes_to_capable_agent(self, clean_registry):
        """Test that delegate() finds and calls the right agent."""
        from etl.agents import BaseAgent
        
        @ray.remote
        class AnalystAgent(BaseAgent):
            CAPABILITIES = ["analysis"]
            def build_executor(self):
                return lambda x: f"Analyzed: {x}"
        
        @ray.remote
        class CoordinatorAgent(BaseAgent):
            CAPABILITIES = ["coordination"]
            def build_executor(self):
                return lambda x: x
            
            def run_analysis(self, data):
                return self.delegate("analysis", data)
        
        # Deploy agents
        analyst = AnalystAgent.options(name="Analyst").remote()
        coordinator = CoordinatorAgent.options(name="Coordinator").remote()
        
        ray.get(analyst.setup.remote())
        ray.get(coordinator.setup.remote())
        
        # Coordinator delegates to Analyst
        result = ray.get(coordinator.run_analysis.remote("test data"))
        assert result == "Analyzed: test data"
        
        # Cleanup
        ray.kill(analyst)
        ray.kill(coordinator)
    
    def test_delegate_retry_on_failure(self, clean_registry):
        """Test that delegation retries with next agent on failure."""
        # Register multiple agents (some might be unavailable)
        ray.get(clean_registry.register.remote("DeadAgent", ["retry_test"]))
        ray.get(clean_registry.register.remote("AliveAgent", ["retry_test"]))
        
        from etl.agents import BaseAgent
        
        @ray.remote
        class AliveTestAgent(BaseAgent):
            CAPABILITIES = ["retry_test"]
            def build_executor(self):
                return lambda x: f"Success: {x}"
        
        # Only deploy the second agent
        alive = AliveTestAgent.options(name="AliveAgent").remote()
        ray.get(alive.setup.remote())
        
        @ray.remote  
        class DelegatorAgent(BaseAgent):
            CAPABILITIES = []
            def build_executor(self):
                return lambda x: x
            
            def test_delegation(self, data):
                return self.delegate("retry_test", data, retry_on_failure=True)
        
        delegator = DelegatorAgent.options(name="Delegator").remote()
        ray.get(delegator.setup.remote())
        
        # Should skip DeadAgent and succeed with AliveAgent
        result = ray.get(delegator.test_delegation.remote("data"))
        assert "Success" in result
        
        # Cleanup
        ray.kill(alive)
        ray.kill(delegator)
