from core.base_ingestion_connector import BaseIngestionConnector, BaseIngestionConfig
from core.utils.dependency_aware_mixin import DependencyAwareMixin
from typing import Optional, Dict, Any
from dataclasses import dataclass
from time import sleep
@dataclass
class AuthConfig:
    """Configuration for API Authentication."""
    type: str = "none"  # "bearer", "header", "param", "none"
    key: str = "Authorization"  # Header key or param name
    token: Optional[str] = None
    username: Optional[str] = None  # For basic auth
    password: Optional[str] = None  # For basic auth

@dataclass
class RestApiIngestionConfig(BaseIngestionConfig):
    """
    Configuration specific to API ingestion.
    """
    headers: Optional[Dict[str, str]] = None  # Additional headers for the API requests
    pagination: Optional[Dict[str, Any]] = None  # Pagination details if applicable
    auth: Optional[AuthConfig] = None  # Authentication configuration
    retries: Optional[int] = 1  # Number of retries for failed requests
    rate_limit_delay: float = 0.0  # Delay between requests to respect rate limits
      

class RestApiIngestion(BaseIngestionConnector, DependencyAwareMixin):
    
    REQUIRED_DEPENDENCIES = ['requests', 'aiohttp']
    
    def __init__(self, ingestion_config: BaseIngestionConfig):
        self.config: RestApiIngestionConfig = ingestion_config
        super().__init__(ingestion_config)
        self._client = None 
        
    @property
    def client(self) -> Any:
        """Escape hatch to get the raw requests.Session object."""
        self._ensure_connected()
        return self._client
        
    def _apply_auth(self):
        """Helper to apply auth logic based on config type."""
        auth = self.config.auth
        if auth.type == "bearer":
            self._client.headers.update({"Authorization": f"Bearer {auth.token}"})
        elif auth.type == "header":
            self._client.headers.update({auth.key: auth.token})
        elif auth.type == "basic":
            self._client.auth = (auth.username, auth.password)
        elif auth.type == "param":
            # Will be merged during fetch
            self._auth_param = {auth.key: auth.token}
    
    
    def connect(self) -> None:
        """
        Lazily creates the HTTP Session on the worker.
        """
        if self._is_connected:
            return

        try:
            import requests
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry
        except ImportError:
            raise ImportError("Missing dependencies. Ensure 'requests' is installed.")

        # 1. Create Session
        self._client = requests.Session()

        # 2. Configure Robust Retries (Essential for ETL)
        retries = Retry(
            total=self.config.retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        self._client.mount('https://', HTTPAdapter(max_retries=retries))
        self._client.mount('http://', HTTPAdapter(max_retries=retries))

        # 3. Apply Global Headers
        self._client.headers.update({"Content-Type": "application/json"})
        if self.config.headers:
            self._client.headers.update(self.config.headers)

        # 4. Apply Authentication
        if self.config.auth:
            self._apply_auth()

        self._is_connected = True
        # Use standard logging in production, print for prototype
        print(f"[RestApiIngestion] Connected to {self.config.base_url}")
        
    def disconnect(self) -> None:
        """Closes the HTTP session."""
        if self._client:
            self._client.close()
        self._client = None
        self._is_connected = False
        
    def fetch_data(self, params: Optional[Dict[str, Any]] = None, **kwargs) -> Iterator[List[Dict[str, Any]]]:
        """
        Public Interface: Fetch data from the REST API.
        Handles pagination automatically based on config.
        
        Args:
            params: Optional dictionary of query parameters to start with.
            **kwargs: Additional arguments if needed by the base interface.
        
        Yields:
            List[Dict[str, Any]]: Batches of records (pages).
        """
        self._ensure_connected()
        params = params or {}
        
        # Read pagination strategy from config
        pag_config = self.config.pagination or {}
        strategy = pag_config.get("type", "none")
        
        current_params = params.copy()

        if strategy == "page":
            # Simple Page Number Pagination
            page_param = pag_config.get("page_param", "page")
            page = 1
            
            while True:
                current_params[page_param] = page
                batch = self._fetch_page(current_params)
                
                if not batch:
                    break # Stop if empty
                
                yield batch
                
                sleep(self.config.rate_limit_delay)
                page += 1
                
        # Add other strategies (cursor, offset) here as needed
        else:
            # Default: Fetch once
            yield self._fetch_page(current_params)

    def _fetch_page(self, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Internal Helper: Fetches a single page/batch of data. 
        """
        self._ensure_connected()
        params = params or {}
        
        # 'endpoint' is internal metadata, not a query param
        endpoint = params.pop("endpoint", "")
        
        # Build URL
        base = self.config.source.rstrip("/")
        path = endpoint.lstrip("/")
        url = f"{base}/{path}"

        # Merge Auth Params if needed
        if hasattr(self, "_auth_param"):
            params.update(self._auth_param)

        try:
            response = self._client.get(url, params=params)
            response.raise_for_status()
            return self._normalize_response(response.json())
        except Exception as e:
            print(f"[RestApiIngestion] Error fetching {url}: {e}")
            raise

    def _normalize_response(self, data: Union[Dict, List]) -> List[Dict[str, Any]]:
        """
        Ensures the return type is always a List of Dictionaries.
        Handles APIs that wrap results in {"data": [...]}
        """
        if isinstance(data, list):
            return data
        
        if isinstance(data, dict):
            # Heuristic: look for common wrapper keys
            for key in ["results", "data", "items", "records"]:
                if key in data and isinstance(data[key], list):
                    return data[key]
            # If no list found, treat the single dict as one record
            return [data]
            
        return []