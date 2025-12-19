from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from prefect import task
from etl.core.utils.dependency_aware_mixin import DependencyAwareMixin

class BaseTask(ABC, DependencyAwareMixin):
    """
    Base class for ETL tasks. 
    Encapsulates business logic and configuration.
    
    Usage:
        class MyTask(BaseTask):
            def run(self, data):
                return data * 2
                
        t = MyTask()
        prefect_task = t.as_task(retries=3)
    """

    def __init__(self, name: Optional[str] = None, config: Dict[str, Any] = None):
        """
        Args:
            name: Name of the task (defaults to Class Name)
            config: Optional configuration dictionary
        """
        self.name = name or self.__class__.__name__
        self.config = config or {}

    @abstractmethod
    def run(self, *args, **kwargs) -> Any:
        """
        The pure Python business logic of the task.
        """
        pass
    
    def as_task(self, **task_kwargs):
        """
        Wraps this instance's run method in a Prefect Task.
        
        Args:
            **task_kwargs: Arguments passed to the @task decorator 
                           (e.g. retries, cache_key_fn, etc.)
        
        Returns:
            A Prefect Task object that can be .submit()-ed in a flow.
        """
        # Default name if not provided in kwargs
        if "name" not in task_kwargs:
            task_kwargs["name"] = self.name

        @task(**task_kwargs)
        def wrapper(*args, **kwargs):
            return self.run(*args, **kwargs)
            
        return wrapper