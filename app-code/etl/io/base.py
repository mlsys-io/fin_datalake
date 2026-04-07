from abc import ABC, abstractmethod
from typing import Iterator, Any, Optional
from etl.core.utils.dependency_aware_mixin import DependencyAwareMixin

class DataReader(ABC, DependencyAwareMixin):
    """
    Active link to a source.
    Usage:
        with source.open() as reader:
            for batch in reader.read_batch(): ...
    """
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    @abstractmethod
    def read_batch(self) -> Iterator[Any]: 
        """Yields batches of data."""
        pass
    
    @abstractmethod
    def close(self): 
        """Closes the connection."""
        pass

class DataWriter(ABC, DependencyAwareMixin):
    """
    Active link to a sink.
    """
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @abstractmethod
    def write_batch(self, data: Any):
        """Writes a batch of data."""
        pass

    @abstractmethod
    def close(self):
        """Closes the connection."""
        pass

class DataSource(ABC, DependencyAwareMixin):
    """
    Describes 'Where data comes from'.
    Safe to pass distributedly (should be pickle-able).
    """
    @abstractmethod
    def open(self) -> DataReader:
        """Creates the runtime reader."""
        pass

class DataSink(ABC, DependencyAwareMixin):
    """
    Describes 'Where data goes'.
    Safe to pass distributedly (should be pickle-able).
    """
    @abstractmethod
    def open(self) -> DataWriter:
        """Creates the runtime writer."""
        pass
