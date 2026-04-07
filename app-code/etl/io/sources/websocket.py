from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any, Iterator
import time
import json
import os

from etl.io.base import DataSource, DataReader

@dataclass
class WebSocketSource(DataSource):
    """
    Configuration for reading from a WebSocket stream (Micro-batching).
    Adapts a live stream into small batches for Ray Actor processing.
    """
    REQUIRED_DEPENDENCIES = ["websocket-client"]

    url: str
    headers: Dict[str, str] = field(default_factory=dict)
    sslopt: Dict[str, Any] = field(default_factory=dict)
    
    # Batching Config
    batch_size: int = 100       # Yield after N messages
    read_timeout: float = 1.0   # Or yield after T seconds

    def open(self) -> 'WebSocketReader':
        return WebSocketReader(self)

class WebSocketReader(DataReader):
    """
    Runtime reader for WebSockets.
    Connects synchronously and buffers messages into batches.
    """

    def __init__(self, source: WebSocketSource):
        self.source = source
        self._ws = None
    
    def _connect(self):
        if self._ws:
            return
            
        import websocket
        import certifi
        sslopt = dict(self.source.sslopt)
        ca_file = str(os.environ.get("WEBSOCKET_CA_CERT") or "").strip()
        if ca_file and "ca_certs" not in sslopt:
            sslopt["ca_certs"] = ca_file
        elif "ca_certs" not in sslopt:
            # Prefer the standard public CA bundle for internet-facing sockets.
            sslopt["ca_certs"] = certifi.where()
        # Note: websocket-client connect is synchronous
        self._ws = websocket.create_connection(
            self.source.url, 
            header=self.source.headers,
            timeout=self.source.read_timeout,
            sslopt=sslopt or None,
        )
        print(f"[WebSocketReader] Connected to {self.source.url}")

    def close(self):
        if self._ws:
            self._ws.close()
            self._ws = None

    def read_batch(self) -> Iterator[List[Dict[str, Any]]]:
        """
        Polls the socket for a window of time/count and yields a batch.
        This is an infinite generator intended to be called in a loop.
        """
        self._connect()
        import websocket

        buffer = []
        start_time = time.time()
        
        # We loop continuously, but yield control back to the ServiceTask periodically
        while True:
            try:
                # Calculate remaining time in the window
                elapsed = time.time() - start_time
                remaining = self.source.read_timeout - elapsed
                
                if len(buffer) >= self.source.batch_size or remaining <= 0:
                    # Batch full or timeout reached: Yield what we have
                    if buffer:
                        yield buffer
                        buffer = []
                    
                    # Reset window
                    start_time = time.time()
                    
                    # If we've yielded, we return control (via yield).
                    # The caller (ServiceTask) assumes next(reader) will be called immediately 
                    # but we want to break the inner loop to allow `yield`.
                    # Actually, since this is a generator, we just 'continue' our internal state 
                    # but we strictly return ONE batch per call usually? 
                    # DataReader.read_batch definition says "Yields batches". 
                    # So yes, yield here is correct.
                    continue

                # Short blocking read
                self._ws.settimeout(max(0.1, remaining)) 
                message = self._ws.recv()
                
                # Parse
                try:
                    data = json.loads(message)
                    buffer.append(data)
                except json.JSONDecodeError:
                    print(f"[WebSocketReader] Warn: Non-JSON message: {message[:50]}...")
                    
            except websocket.WebSocketTimeoutException:
                # Timeout on recv() is fine, just loop to check window
                pass
            except (websocket.WebSocketConnectionClosedException, ConnectionResetError) as e:
                print(f"[WebSocketReader] Connection Lost: {e}. Reconnecting...")
                self.close()
                time.sleep(1) # Backoff
                self._connect()
            except Exception as e:
                print(f"[WebSocketReader] Unexpected error: {e}")
                raise
