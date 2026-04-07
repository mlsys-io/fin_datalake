"""
HTTP Sink for webhook and API notifications.
Heavy imports are deferred to runtime for Ray worker execution.
"""
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Union, TYPE_CHECKING

from etl.io.base import DataSink, DataWriter

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa


@dataclass
class HttpSink(DataSink):
    """
    Configuration for HTTP/Webhook notifications.
    
    Example:
        sink = HttpSink(
            url="https://api.example.com/webhook",
            auth_token="secret-token",
            batch_key="events"
        )
        
        with sink.open() as writer:
            writer.write_batch([{"event": "order", "id": 123}])
    """
    REQUIRED_DEPENDENCIES = ["requests"]
    
    url: str
    method: str = "POST"
    headers: Dict[str, str] = field(default_factory=dict)
    auth_token: Optional[str] = None
    auth_type: str = "bearer"  # "bearer", "header", "basic"
    auth_header_name: str = "Authorization"
    
    # Payload configuration
    batch_key: Optional[str] = "data"
    include_metadata: bool = False
    
    # Request configuration
    timeout: float = 30.0
    retries: int = 3
    retry_backoff: float = 1.0
    
    # Response handling
    raise_on_error: bool = True
    
    def __post_init__(self):
        """Validate URL format."""
        from urllib.parse import urlparse
        parsed = urlparse(self.url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(f"Invalid URL format: {self.url}")
        if self.auth_type not in ["bearer", "header", "basic", "none"]:
            raise ValueError(f"auth_type must be 'bearer', 'header', 'basic', or 'none'")
    
    def open(self) -> 'HttpWriter':
        return HttpWriter(self)


class HttpWriter(DataWriter):
    """
    Runtime HTTP client for sending data to webhooks/APIs.
    """
    
    def __init__(self, sink: HttpSink):
        self.sink = sink
        self._session = None
    
    def _get_session(self):
        if self._session:
            return self._session
        
        try:
            import requests
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry
        except ImportError:
            raise ImportError(
                "HttpSink requires 'requests'. "
                "Install with: pip install requests"
            )
        
        self._session = requests.Session()
        
        # Configure retries
        retries = Retry(
            total=self.sink.retries,
            backoff_factor=self.sink.retry_backoff,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        self._session.mount('https://', HTTPAdapter(max_retries=retries))
        self._session.mount('http://', HTTPAdapter(max_retries=retries))
        
        # Set headers
        self._session.headers.update({"Content-Type": "application/json"})
        self._session.headers.update(self.sink.headers)
        
        # Configure authentication
        if self.sink.auth_token:
            if self.sink.auth_type == "bearer":
                self._session.headers[self.sink.auth_header_name] = f'Bearer {self.sink.auth_token}'
            elif self.sink.auth_type == "header":
                self._session.headers[self.sink.auth_header_name] = self.sink.auth_token
            elif self.sink.auth_type == "basic":
                import base64
                encoded = base64.b64encode(self.sink.auth_token.encode()).decode()
                self._session.headers["Authorization"] = f'Basic {encoded}'
        
        return self._session
    
    def write_batch(self, data: Union[Any, Any, List[Dict[str, Any]]]):
        """
        Send a batch of data to the configured HTTP endpoint.
        """
        # Heavy imports inside method - executes on Ray worker
        import pandas as pd
        import pyarrow as pa
        from loguru import logger
        
        session = self._get_session()
        
        # Normalize to list of dicts
        records = self._normalize_data(data)
        
        if not records:
            logger.debug("Empty batch, skipping HTTP send")
            return
        
        # Build payload
        payload = self._build_payload(records)
        
        try:
            resp = session.request(
                method=self.sink.method,
                url=self.sink.url,
                json=payload,
                timeout=self.sink.timeout
            )
            
            if self.sink.raise_on_error:
                resp.raise_for_status()
            
            logger.info(f"Sent {len(records)} records to {self.sink.url} (status: {resp.status_code})")
            
        except Exception as e:
            logger.error(f"HTTP send failed to {self.sink.url}: {e}")
            if self.sink.raise_on_error:
                raise
    
    def _normalize_data(self, data) -> List[Dict[str, Any]]:
        """Convert various data types to list of dicts."""
        import pandas as pd
        import pyarrow as pa
        
        if isinstance(data, pd.DataFrame):
            return data.to_dict('records')
        elif isinstance(data, pa.Table):
            return data.to_pylist()
        elif isinstance(data, list):
            return data
        else:
            raise ValueError(f"Unsupported data type for HttpWriter: {type(data)}")
    
    def _build_payload(self, records: List[Dict]) -> Union[Dict, List]:
        """Build the JSON payload to send."""
        if self.sink.batch_key:
            payload = {self.sink.batch_key: records}
            
            if self.sink.include_metadata:
                import datetime
                payload["_metadata"] = {
                    "count": len(records),
                    "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
                }
            
            return payload
        else:
            return records
    
    def close(self):
        """Close the HTTP session."""
        if self._session:
            try:
                self._session.close()
            except Exception as e:
                from loguru import logger
                logger.warning(f"Error closing HTTP session: {e}")
            finally:
                self._session = None
