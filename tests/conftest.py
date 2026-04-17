"""
Global Pytest setup and shared fixtures.

This file is automatically discovered by Pytest. Fixtures defined here
are available to all test files without needing to be imported.
"""
from pathlib import Path
from dataclasses import dataclass
import types
import sys

import pytest
import ray


APP_CODE_DIR = Path(__file__).resolve().parents[1] / "app-code"
if str(APP_CODE_DIR) not in sys.path:
    sys.path.insert(0, str(APP_CODE_DIR))


def _install_prefect_stubs() -> None:
    if "prefect" in sys.modules:
        return

    prefect_module = types.ModuleType("prefect")

    @dataclass
    class _Result:
        value: object

        def result(self):
            return self.value

    class _Task:
        def __init__(self, fn):
            self.fn = fn
            self.__name__ = getattr(fn, "__name__", "task")

        def __call__(self, *args, **kwargs):
            return self.fn(*args, **kwargs)

        def submit(self, *args, **kwargs):
            return _Result(self.fn(*args, **kwargs))

    def task(*decorator_args, **decorator_kwargs):
        def decorator(fn):
            return _Task(fn)

        if decorator_args and callable(decorator_args[0]) and len(decorator_args) == 1 and not decorator_kwargs:
            return decorator(decorator_args[0])
        return decorator

    def flow(*decorator_args, **decorator_kwargs):
        def decorator(fn):
            def wrapper(*args, **kwargs):
                return fn(*args, **kwargs)

            wrapper.fn = fn  # type: ignore[attr-defined]
            wrapper.__name__ = getattr(fn, "__name__", "flow")
            return wrapper

        if decorator_args and callable(decorator_args[0]) and len(decorator_args) == 1 and not decorator_kwargs:
            return decorator(decorator_args[0])
        return decorator

    prefect_module.task = task
    prefect_module.flow = flow

    prefect_ray_module = types.ModuleType("prefect_ray")
    task_runners_module = types.ModuleType("prefect_ray.task_runners")
    prefect_client_module = types.ModuleType("prefect.client")
    prefect_orchestration_module = types.ModuleType("prefect.client.orchestration")

    class _PrefectClientContext:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    def get_client():
        return _PrefectClientContext()

    class RayTaskRunner:
        def __init__(self, address=None, **kwargs):
            self.address = address
            self.kwargs = kwargs

    task_runners_module.RayTaskRunner = RayTaskRunner
    prefect_orchestration_module.get_client = get_client
    prefect_client_module.orchestration = prefect_orchestration_module
    prefect_ray_module.task_runners = task_runners_module

    sys.modules["prefect"] = prefect_module
    sys.modules["prefect.client"] = prefect_client_module
    sys.modules["prefect.client.orchestration"] = prefect_orchestration_module
    sys.modules["prefect_ray"] = prefect_ray_module
    sys.modules["prefect_ray.task_runners"] = task_runners_module


_install_prefect_stubs()


def _install_langchain_core_stub() -> None:
    if "langchain_core" in sys.modules:
        return

    langchain_core_module = types.ModuleType("langchain_core")
    runnables_module = types.ModuleType("langchain_core.runnables")

    class _RunnableChain:
        def __init__(self, steps):
            self._steps = list(steps)

        def invoke(self, value):
            result = value
            for step in self._steps:
                if hasattr(step, "invoke") and callable(step.invoke):
                    result = step.invoke(result)
                else:
                    result = step(result)
            return result

        def __or__(self, other):
            return _RunnableChain([*self._steps, other])

    class RunnableLambda:
        def __init__(self, func):
            self.func = func

        def invoke(self, value):
            return self.func(value)

        def __call__(self, value):
            return self.func(value)

        def __or__(self, other):
            return _RunnableChain([self, other])

    runnables_module.RunnableLambda = RunnableLambda
    langchain_core_module.runnables = runnables_module

    sys.modules["langchain_core"] = langchain_core_module
    sys.modules["langchain_core.runnables"] = runnables_module


_install_langchain_core_stub()


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
