from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Iterator, Union
from time import sleep

from etl.io.base import DataSource, DataReader

# --- Configuration (Serializable) ---

@dataclass
class AuthConfig:
    type: str = "none"  # "bearer", "header", "param", "basic", "none"
    key: str = "Authorization"
    token: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None

@dataclass
class PaginationConfig:
    type: str = "none" # "page", "cursor", "none"
    page_param: str = "page"

@dataclass
class RestApiSource(DataSource):
    """
   Configuration for REST API Data Source.
    """
    REQUIRED_DEPENDENCIES = ["requests"]

    url: str
    method: str = "GET"
    headers: Dict[str, str] = field(default_factory=dict)
    auth: Optional[AuthConfig] = None
    pagination: Optional[PaginationConfig] = None
    retries: int = 3
    rate_limit_delay: float = 0.0
    
    def __post_init__(self):
        """Validate URL format."""
        from urllib.parse import urlparse
        parsed = urlparse(self.url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(f"Invalid URL format: {self.url}. Must include scheme (http/https) and domain.")

    def open(self) -> 'RestApiReader':
        return RestApiReader(self)

# --- Runtime (Active Connection) ---

class RestApiReader(DataReader):
    """
    Runtime reader that holds the Requests Session.
    """
    def __init__(self, source: RestApiSource):
        self.source = source
        self._session = None

    def _ensure_session(self):
        if self._session:
            return
        
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        from loguru import logger
        
        self._session = requests.Session()
        
        # Configure Retries
        retries = Retry(
            total=self.source.retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        self._session.mount('https://', HTTPAdapter(max_retries=retries))
        self._session.mount('http://', HTTPAdapter(max_retries=retries))
        
        # Headers
        self._session.headers.update({"Content-Type": "application/json"})
        self._session.headers.update(self.source.headers)
        
        # Auth
        if self.source.auth:
            self._apply_auth()
            
        logger.info(f"Connected to {self.source.url}")

    def _apply_auth(self):
        auth = self.source.auth
        if auth.type == "bearer":
            self._session.headers.update({"Authorization": f"Bearer {auth.token}"})
        elif auth.type == "header":
            self._session.headers.update({auth.key: auth.token})
        elif auth.type == "basic":
            self._session.auth = (auth.username, auth.password)
        # "param" auth is handled during request construction if needed, 
        # or we could inject it into a default params dict here.

    def close(self):
        if self._session:
            self._session.close()
            self._session = None
            
    def read_batch(self) -> Iterator[List[Dict[str, Any]]]:
        """
        Yields pages of results.
        """
        self._ensure_session()
        
        params = {} # Could be extended to support initial params
        
        # Pagination Logic
        pag_type = self.source.pagination.type if self.source.pagination else "none"
        
        if pag_type == "page":
            page_param = self.source.pagination.page_param
            page = 1
            while True:
                # Use a fresh copy for each request to avoid mutating state
                current_params = params.copy()
                current_params[page_param] = page
                
                data = self._fetch_one(current_params)
                
                if not data:
                    break
                
                yield data
                
                sleep(self.source.rate_limit_delay)
                page += 1
                
        else:
            # Fetch once
            yield self._fetch_one(params)

    def _fetch_one(self, params: Dict) -> List[Dict]:
        from loguru import logger
        try:
            resp = self._session.request(
                method=self.source.method,
                url=self.source.url,
                params=params
            )
            resp.raise_for_status()
            return self._normalize(resp.json())
        except Exception as e:
            logger.error(f"Error fetching {self.source.url}: {e}")
            raise

    def _normalize(self, data: Union[Dict, List]) -> List[Dict]:
        """
        Extract list from common API response structures.
        
        Handles three response patterns:
        1. Direct list: [{...}, {...}] -> return as-is
        2. Nested list: {"results": [...]} or {"data": [...]} or {"items": [...]}
        3. Single object: {...} -> wrap in list
        
        Args:
            data: Raw JSON response from API
            
        Returns:
            List of dictionaries representing data records
        """
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for k in ["results", "data", "items"]:
                if k in data and isinstance(data[k], list):
                    return data[k]
            return [data]
        return []
