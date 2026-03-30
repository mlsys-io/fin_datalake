from typing import Any, Dict
from etl.core.base_service import ServiceTask
from etl.io.sources.websocket import WebSocketSource
from etl.io.sinks.timescaledb import TimescaleDBSink

class WebSocketIngestorService(ServiceTask):
    """
    A Ray Actor service that continuously ingests from a WebSocket
    and writes to TimescaleDB.
    """
    REQUIRED_DEPENDENCIES = ["websocket-client", "psycopg2"]

    def __init__(self, name: str = None, config: Dict = None, **kwargs):
        super().__init__(name, config=config, **kwargs)
        self.source_config = self.config.get("source", {})
        self.sink_config = self.config.get("sink", {})
        self.running = False

    def run(self):
        """
        The main event loop of the actor.
        """
        print(f"[{self.name}] Service Starting...")
        self.running = True

        # 1. Instantiate Components
        source = WebSocketSource(**self.source_config)
        sink = TimescaleDBSink(**self.sink_config)

        # 2. Open Connections
        # We use context managers to ensure cleanup
        with source.open() as reader, sink.open() as writer:
            print(f"[{self.name}] Connections established. Entering loop.")
            
            # 3. Micro-Batch Loop
            # reader.read_batch() yields indefinitely for WebSockets
            for batch in reader.read_batch():
                if not self.running:
                    print(f"[{self.name}] Stop signal received.")
                    break
                
                if not batch:
                    continue

                try:
                    # Optional: Processing / Transformation
                    # processed_batch = [self.transform(x) for x in batch]
                    
                    # 4. Write to Sink
                    writer.write_batch(batch)
                    
                    from loguru import logger
                    # Update status (Ray Actor state)
                    logger.info(f"[{self.name}] Ingested {len(batch)} messages")
                    
                except Exception as e:
                    print(f"[{self.name}] Error in batch loop: {e}")
                    # Decide: Continue or Crash? For service, usually continue.
                    continue

    def stop(self):
        self.running = False
        print(f"[{self.name}] Stopping...")
