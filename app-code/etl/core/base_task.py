from abc import ABC, abstractmethod
from typing import Any, Dict

class BaseTask(ABC):
    """
    Base class for all ETL tasks.
    Users should extend this class and implement the `run` method.
    """

    def __init__(self, name: str, config: Dict[str, Any] = None):
        """
        Args:
            name: Name of the task
            config: Optional configuration dictionary
        """
        self.name = name
        self.config = config or {}

    @abstractmethod
    def run(self, *args, **kwargs) -> Any:
        """
        The main logic of the task goes here.
        Must be implemented by subclass.
        """
        pass

    def pre_run(self):
        """Optional hook before run (logging, validation, metrics)."""
        print(f"[{self.name}] Pre-run hook.")

    def post_run(self, result: Any):
        """Optional hook after run (logging, metrics, cleanup)."""
        print(f"[{self.name}] Post-run hook. Result: {result}")
        return result

    def execute(self, *args, **kwargs) -> Any:
        """
        Standardized way to run the task:
        Calls pre_run -> run -> post_run
        """
        self.pre_run()
        result = self.run(*args, **kwargs)
        return self.post_run(result)
