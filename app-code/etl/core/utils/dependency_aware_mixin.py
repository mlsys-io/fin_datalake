from typing import List, Set

class DependencyAwareMixin:
    """
    Mixin class that provides functionality to manage dependencies
    between different components in an ETL pipeline.
    """

    REQUIRED_DEPENDENCIES: List[str] = []
    
    @classmethod
    def get_required_dependencies(cls) -> Set[str]:
        
        return set(cls.REQUIRED_DEPENDENCIES)