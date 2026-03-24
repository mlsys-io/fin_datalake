import pytest
import ray
from .mock_agent import MockAgent


@pytest.fixture(scope="module")
def ray_cluster():
    if not ray.is_initialized():
        # Use local_mode to get direct tracebacks for debugging init failures
        ray.init(
            ignore_reinit_error=True, 
            local_mode=True
        )
    yield
    if ray.is_initialized():
        ray.shutdown()

def test_base_agent_standalone(ray_cluster):
    # Deploy the agent
    agent_handle = MockAgent.deploy(name="TestMockAgent")
    
    # Verify it doesn't depend on ServiceTask properties
    # but still has the necessary methods
    assert hasattr(agent_handle, "ask")
    assert hasattr(agent_handle, "setup")
    assert hasattr(agent_handle, "shutdown")
    
    # Test synchronous ask execution
    result = agent_handle.ask("test")
    assert result == "MOCK_TEST"
    
    # Clean up
    agent_handle.shutdown()

def test_base_agent_direct_call():
    # Verify we can also instantiate it normally (without Ray)
    agent = MockAgent()
    agent.setup()
    
    result = agent.ask("direct_test")
    assert result == "MOCK_DIRECT_TEST"
