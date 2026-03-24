"""
Hot-Reload Engine — Safely compiles and instantiates custom Overseer policies.
"""
import os
import inspect
import importlib.util
from loguru import logger
from typing import List

from overseer.policies.base import BasePolicy

def load_custom_policies(local_dir: str) -> List[BasePolicy]:
    """
    Scans a directory for .py files, safely compiles them, and extracts 
    any classes that inherit from BasePolicy.
    """
    if not os.path.exists(local_dir):
        return []

    policies = []
    
    for filename in os.listdir(local_dir):
        if filename.endswith(".py") and not filename.startswith("_"):
            filepath = os.path.join(local_dir, filename)
            module_name = f"custom_policy_{filename.replace('.py', '')}"
            
            try:
                # Safely load the module
                spec = importlib.util.spec_from_file_location(module_name, filepath)
                if spec is None or spec.loader is None:
                    continue
                    
                module = importlib.util.module_from_spec(spec)
                
                # Execution barrier: catches SyntaxErrors, NameErrors, etc.
                spec.loader.exec_module(module)  # type: ignore
                
                # Find all classes in the compiled module
                for name, obj in inspect.getmembers(module, inspect.isclass):
                    # Ensure it's a BasePolicy subclass but NOT BasePolicy itself
                    if issubclass(obj, BasePolicy) and obj is not BasePolicy:
                        # Ensure it was actually defined in this module (not imported)
                        if obj.__module__ == module_name:
                            try:
                                # Instantiate the policy object safely
                                instance = obj()
                                policies.append(instance)
                                logger.info(f"Successfully loaded custom policy: {name} from {filename}")
                            except Exception as init_err:
                                logger.error(f"Failed to instantiate custom policy {name} from {filename}: {init_err}")
                                
            except Exception as e:
                # Catch ALL errors (SyntaxError, runtime errors during exec_module, etc.)
                logger.error(f"Critical error compiling custom policy file {filename}: {e}. Skipping file completely.")
                
    return policies
