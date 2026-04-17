import pytest
import ray
from ray import serve
from .mock_agent import MockAgent


@pytest.fixture(scope="module")
def ray_cluster():
    if not ray.is_initialized():
        ray.init(
            ignore_reinit_error=True, 
        )
    yield
    if ray.is_initialized():
        serve.shutdown()
        ray.shutdown()

def test_base_agent_standalone(ray_cluster):
    # Deploy the agent
    agent_handle = MockAgent.deploy(name="TestMockAgent")
    
    # Verify it doesn't depend on ServiceTask properties
    # but still has the necessary methods
    assert hasattr(agent_handle, "ask")
    assert hasattr(agent_handle, "setup")
    assert hasattr(agent_handle, "shutdown")
    
    # Test Serve handle execution
    result = agent_handle.ask.remote("test").result()
    assert result == "MOCK_TEST"
    
    # Clean up
    agent_handle.shutdown()

def test_base_agent_direct_call():
    # Verify we can also instantiate it normally (without Ray)
    agent = MockAgent()
    agent.setup()
    
    result = agent.ask("direct_test")
    assert result == "MOCK_DIRECT_TEST"
