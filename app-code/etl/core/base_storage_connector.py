from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Union
from dataclasses import dataclass


@dataclass
class BaseStorageLocation:
    """
    Represents a location in a storage system.
    This is storage-agnostic and connector-specific parsing handles details.
    """
    uri: str  # e.g., "s3://bucket/path", "gs://bucket/path", "/local/path"
    
    # Optional metadata
    partition_keys: Optional[Dict[str, Any]] = None
    schema: Optional[Dict[str, str]] = None  # column_name -> data_type
    

@dataclass
class BaseReadOptions:
    """
    Options for reading data from storage.
    """
    buffer_size: Optional[int] = None  # in bytes
    encoding: Optional[str] = None
    timeout: Optional[int] = None  # in seconds
    
    
@dataclass
class BaseWriteOptions:
    """
    Options for writing data to storage.
    """
    buffer_size: Optional[int] = None  # in bytes
    encoding: Optional[str] = None
    timeout: Optional[int] = None  # in seconds
    overwrite: bool = False
    
@dataclass
class BaseMetadata:
    """
    Metadata information for a storage object.
    """
    size: Optional[int] = None  # in bytes
    last_modified: Optional[str] = None  # ISO formatted datetime string
    content_type: Optional[str] = None
    

class BaseStorageConnector(ABC):
    """
    Base class for all storage connectors.
    """

    def __init__(self, connection_config: Dict[str, Any] = None, **kwargs):
        self.connection_config = connection_config or {}
        self.options = kwargs  
        
        self._client = None
        self._is_connected = False

    @abstractmethod
    def connect(self):
        """
        Establish connection to the storage system.
        """
        pass

    @abstractmethod
    def disconnect(self):
        """
        Close connection to the storage system.
        """
        pass
    
    # To use 'with' statement
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.disconnect()
        return False  # Do not suppress exceptions
    
    def client(self):
        """
        Returns the underlying client/connection object.
        """
        if not self._is_connected:
            self.connect()
            
        return self._client
    
    # --------------------------------------------------------------------------
    # Read Operations
    # --------------------------------------------------------------------------
    
    def read_bytes(self, location: Union[str, BaseStorageLocation], option: BaseReadOptions) -> None:
        """
        Read bytes from the specified path in storage.
        """
        raise NotImplementedError("read_bytes method not implemented.")
    
    
    def read_stream(self, location: Union[str, BaseStorageLocation], option: BaseReadOptions) -> None:
        """
        Read a stream from the specified path in storage.
        """
        raise NotImplementedError("read_stream method not implemented.")
    
    
    def read_dataframe(self, location: Union[str, BaseStorageLocation], option: BaseReadOptions) -> None:
        """
        Read a DataFrame from the specified path in storage.
        """
        raise NotImplementedError("read_data_frame method not implemented.")
    
    
    def read_text(self, location: Union[str, BaseStorageLocation], option: BaseReadOptions) -> None:
        """
        Read text from the specified path in storage.
        """
        raise NotImplementedError("read_text method not implemented.")
    
    # --------------------------------------------------------------------------
    # Write Operations
    # --------------------------------------------------------------------------
    
    def write_bytes(self, data: bytes, location: Union[str, BaseStorageLocation], option: BaseWriteOptions) -> None:
        """
        Write bytes to the specified path in storage.
        """
        raise NotImplementedError("write_bytes method not implemented.")
    
    def write_stream(self, data: Any, location: Union[str, BaseStorageLocation], option: BaseWriteOptions) -> None:
        """
        Write a stream to the specified path in storage.
        """
        raise NotImplementedError("write_stream method not implemented.")
    
    def write_dataframe(self, data: Any, location: Union[str, BaseStorageLocation], option: BaseWriteOptions) -> None:
        """
        Write a DataFrame to the specified path in storage.
        """
        raise NotImplementedError("write_data_frame method not implemented.")
    
    def write_text(self, data: str, location: Union[str, BaseStorageLocation], option: BaseWriteOptions) -> None:
        """
        Write text to the specified path in storage.
        """
        raise NotImplementedError("write_text method not implemented.")
    
    
    # --------------------------------------------------------------------------
    # Metadata Operations
    # --------------------------------------------------------------------------
    
    def exists(self, location: Union[str, BaseStorageLocation]) -> bool:
        """
        Check if the specified path exists in storage.
        """
        raise NotImplementedError("exists method not implemented.")
    
    def get_metadata(self, location: Union[str, BaseStorageLocation]) -> BaseMetadata:
        """
        Get metadata of the specified path in storage.
        """
        raise NotImplementedError("get_metadata method not implemented.")
    
    # --------------------------------------------------------------------------
    # Data Management Operations
    # --------------------------------------------------------------------------
    
    def delete(self, location: Union[str, BaseStorageLocation]) -> bool:
        """
        Delete the specified path from storage.
        """
        raise NotImplementedError("delete method not implemented.")
    
    
    def copy(self, source: Union[str, BaseStorageLocation], destination: Union[str, BaseStorageLocation]) -> bool:
        """
        Copy data from source to destination in storage.
        """
        raise NotImplementedError("copy method not implemented.")
    
    def move(self, source: Union[str, BaseStorageLocation], destination: Union[str, BaseStorageLocation]) -> bool:
        """
        Move data from source to destination in storage.
        """
        raise NotImplementedError("move method not implemented.")
    
    
    def create_directory(self, location: Union[str, BaseStorageLocation]) -> bool:
        """
        Create a directory at the specified path in storage.
        """
        raise NotImplementedError("create_directory method not implemented.")