import pytest
from prefect import flow
from etl.core.base_task import BaseTask

class SimpleTask(BaseTask):
    REQUIRED_DEPENDENCIES = ["dummy_pkg"]
    
    def run(self, x: int):
        return x + self.config.get("add", 0)

def test_base_task_initialization():
    t = SimpleTask(config={"add": 5})
    assert t.config["add"] == 5
    assert t.name == "SimpleTask"
    assert "dummy_pkg" in t.get_required_dependencies()

def test_base_task_execution_logic():
    t = SimpleTask(config={"add": 10})
    assert t.run(5) == 15

def test_as_task_wrapper():
    # Test that it creates a valid prefect task
    t = SimpleTask(config={"add": 1})
    prefect_task = t.as_task()
    
    @flow
    def test_flow():
        return prefect_task.submit(1).result()
        
    result = test_flow()
    assert result == 2
