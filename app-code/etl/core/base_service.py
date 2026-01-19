from typing import Any, Dict, Optional, Type
from abc import abstractmethod
import ray
from etl.core.base_task import BaseTask

class ServiceTask(BaseTask):
    """
    A Task designed to run indefinitely as a Service (e.g., Kafka Consumer).
    intended to be deployed as a Ray Actor.
    """
    
    @abstractmethod
    def run(self, *args, **kwargs) -> Any:
        """
        The main loop of the service. 
        Should typically be an infinite loop.
        """
        pass
    
    def as_ray_actor(self, **actor_options) -> Type:
        """
        Converts this ServiceTask into a Ray Actor Class.
        
        Args:
            **actor_options: Options passed to @ray.remote (e.g., num_cpus=1, max_concurrency=...).
            
        Returns:
            A Ray Actor Class that can be instantiated with .remote().
        """
        
        # We wrap the logic in a Ray Actor class
        class ServiceActor:
            def __init__(inner_self, config: Dict[str, Any] = None):
                # Instantiate the actual ServiceTask logic
                # We map the actor init args to the Task init
                self.config = config or {}
                # Manually init the core task with config
                # Since we are inside the actor, 'self' is the Actor, but we want to run the Task logic.
                # A cleaner way is to have the Task BE the Actor logic, but Ray requires a Class.
                # So we proxy.
                self.task_instance = self.__class__.TaskClass(name=self.__class__.TaskClass.__name__, config=self.config)
            
            def run(inner_self, *args, **kwargs):
                return inner_self.task_instance.run(*args, **kwargs)
                
            def stop(inner_self):
                if hasattr(inner_self.task_instance, "stop"):
                    inner_self.task_instance.stop()
                    
            def get_status(inner_self):
                """
                Returns the current status dictionary of the service logic.
                """
                if hasattr(inner_self.task_instance, "get_status"):
                    return inner_self.task_instance.get_status()
                return {"status": "unknown"}

        # Link the helper class to the specific Task subclass
        ServiceActor.TaskClass = self.__class__
        ServiceActor.__name__ = f"{self.__class__.__name__}Actor"
        
        return ray.remote(**actor_options)(ServiceActor)

    def get_status(self) -> Dict[str, Any]:
        """
        Default status implementation. Can be overridden.
        """
        return {"running": getattr(self, "running", False)}
