"""
Global Pytest setup and shared fixtures.

This file is automatically discovered by Pytest. Fixtures defined here
are available to all test files without needing to be imported.
"""
import pytest
import ray


@pytest.fixture(scope="session")
def ray_cluster():
    """
    Initialize a local Ray cluster once for the entire test session.
    All integration tests will share this instance.
    """
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True, num_cpus=4)
    yield
    # Deliberately not shutting down so it's faster for local dev iteration,
    # but in a formal CI environment you might add ray.shutdown() here.


@pytest.fixture
def clean_hub(ray_cluster):
    """
    Ensure a clean AgentHub state before each test.
    Automatically fetches the singleton Hub and unregisters all known agents.
    """
    from etl.agents.hub import get_hub
    
    hub_handle = get_hub()
    
    # Clear any existing agents from previous tests
    agents = ray.get(hub_handle.list_agents.remote())
    for agent_info in agents:
        ray.get(hub_handle.unregister.remote(agent_info["name"]))
    
    yield hub_handle
