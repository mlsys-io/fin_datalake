from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from functools import wraps
from prefect import task
from etl.core.utils.dependency_aware_mixin import DependencyAwareMixin


class BaseTask(ABC, DependencyAwareMixin):
    """
    Base class for all tasks (ETL, AI, user jobs).
    Encapsulates business logic and provides clean execution API.
    
    Usage:
        class MyTask(BaseTask):
            def run(self, data):
                return data * 2
                
        t = MyTask(name="Double Data", retries=3)
        
        # Distributed execution (via TaskRunner)
        result = t(data)           # Clean API
        
        # Local execution (for I/O with runtime conflicts)
        result = t.local(data)     # Runs on driver
        
        # Advanced usage
        prefect_task = t.as_task()
        prefect_task.submit(data)
    """

    def __init__(
        self, 
        name: Optional[str] = None, 
        config: Dict[str, Any] = None,
        retries: int = 0,
        **task_kwargs
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
        
        # Cache the Prefect tasks
        self._distributed_task = None
        self._local_task = None

    @abstractmethod
    def run(self, *args, **kwargs) -> Any:
        """
        Pure Python business logic. Override this in subclasses.
        """
        pass
    
    # =========================================================================
    # Clean API - Primary methods users should call
    # =========================================================================
    
    def __call__(self, *args, **kwargs):
        """
        Execute task via TaskRunner (distributed).
        
        Returns a Future that can be .result()-ed or passed to other tasks.
        """
        return self.as_task().submit(*args, **kwargs)
    
    def local(self, *args, **kwargs):
        """
        Execute task on driver (local, not distributed).
        
        Use for I/O tasks that have runtime conflicts (e.g., Delta Lake + Ray).
        Returns the result directly (not a Future).
        """
        return self.as_local_task()(*args, **kwargs)
    
    # =========================================================================
    # Advanced API - For customization
    # =========================================================================
    
    def as_task(self, **override_kwargs):
        """
        Returns Prefect Task for distributed execution via TaskRunner.
        
        Args:
            **override_kwargs: Override task settings for this call
            
        Returns:
            Prefect Task that can be .submit()-ed
        """
        # Merge kwargs: instance defaults < override
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
    
    def as_local_task(self, **override_kwargs):
        """
        Returns Prefect Task for local execution (runs on driver).
        
        Use for tasks that have runtime conflicts with distributed execution
        (e.g., Delta Lake's Tokio runtime vs Ray's async executor).
        
        Args:
            **override_kwargs: Override task settings for this call
            
        Returns:
            Prefect Task that runs locally (no TaskRunner)
        """
        # Merge kwargs
        merged_kwargs = {
            "name": f"{self.name} (local)",
            "retries": self.retries,
            **self.task_kwargs,
            **override_kwargs,
        }
        
        task_instance = self
        
        # Create task without TaskRunner integration
        @task(**merged_kwargs)
        @wraps(self.run)
        def local_wrapper(*args, **kwargs):
            return task_instance.run(*args, **kwargs)
        
        local_wrapper.__name__ = f"{self.name.replace(' ', '_')}_local"
        local_wrapper.__qualname__ = f"{self.__class__.__name__}.{self.name}.local"
        
        return local_wrapper