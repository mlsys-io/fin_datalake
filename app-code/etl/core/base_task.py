from abc import ABC, abstractmethod
from functools import wraps
from typing import Any, Dict, Optional

from prefect import task

from etl.core.utils.dependency_aware_mixin import DependencyAwareMixin


class BaseTask(ABC, DependencyAwareMixin):
    """
    Base class for finite units of work executed through Prefect.

    Usage:
        class MyTask(BaseTask):
            def run(self, data):
                return data * 2

        t = MyTask(name="Double Data", retries=3)

        future = t.submit(data)    # Distributed execution
        result = t.local(data)     # Direct local execution

        prefect_task = t.as_task() # Advanced Prefect access
        prefect_task.submit(data)
    """

    def __init__(
        self,
        name: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        retries: int = 0,
        **task_kwargs,
    ):
        """
        Args:
            name: Task name (defaults to class name)
            config: Optional configuration dictionary
            retries: Number of retries on failure
            **task_kwargs: Additional args passed to Prefect @task decorator
        """
        self.name = name or self.__class__.__name__
        self.config = config or {}
        self.retries = retries
        self.task_kwargs = task_kwargs

    @abstractmethod
    def run(self, *args, **kwargs) -> Any:
        """
        Pure Python business logic. Override this in subclasses.
        """
        pass

    def submit(self, *args, **kwargs):
        """
        Submit the task for Prefect-managed execution.

        Returns a future that can be .result()-ed or passed to other tasks.
        """
        return self.as_task().submit(*args, **kwargs)

    def local(self, *args, **kwargs):
        """
        Execute task directly on the current process.

        Use for I/O tasks that have runtime conflicts (e.g., Delta Lake + Ray).
        Returns the result directly.
        """
        return self.run(*args, **kwargs)

    def as_task(self, **override_kwargs):
        """
        Return a Prefect Task wrapper around run().

        Args:
            **override_kwargs: Override task settings for this call

        Returns:
            Prefect Task that can be .submit()-ed
        """
        merged_kwargs = {
            "name": self.name,
            "retries": self.retries,
            **self.task_kwargs,
            **override_kwargs,
        }

        task_instance = self

        @task(**merged_kwargs)
        @wraps(self.run)
        def wrapper(*args, **kwargs):
            return task_instance.run(*args, **kwargs)

        wrapper.__name__ = self.name.replace(" ", "_")
        wrapper.__qualname__ = f"{self.__class__.__name__}.{self.name}"
        return wrapper
