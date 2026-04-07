from typing import Dict, Any
import time
from collections import defaultdict

from etl.core.base_service import ServiceTask
from etl.io.sources.websocket import WebSocketSource
from etl.io.sinks.timescaledb import TimescaleDBSink

class StatefulProcessorService(ServiceTask):
    """
    Stateful Ray Actor that ingests streaming data, 
    calculates windowed aggregates (Count/Sum), 
    and writes results to TimescaleDB.
    """
    REQUIRED_DEPENDENCIES = ["websocket-client", "psycopg2"]

    def __init__(self, name: str = None, config: Dict = None, 
                 source_config: Dict = None, sink_config: Dict = None, 
                 window_seconds: int = 10):
        config = config or {}
        super().__init__(name=name or self.__class__.__name__, config=config)
        self.source_config = source_config or config.get("source_config", {})
        self.sink_config = sink_config or config.get("sink_config", {})
        self.window_seconds = window_seconds or config.get("window_seconds", 10)
        
        # State: {symbol: {count: int, total_price: float}}
        self.state = defaultdict(lambda: {"count": 0, "sum_price": 0.0})
        self.metrics = {"total_processed": 0, "last_flush_count": 0, "last_flush_time": None}
        self.last_flush_time = time.time()
        self.running = False

    def run(self):
        if not self._begin_run_loop():
            return

        print(f"[{self.name}] Stateful Processor Starting (Window: {self.window_seconds}s)...")

        source = WebSocketSource(**self.source_config)
        sink = TimescaleDBSink(**self.sink_config)

        try:
            with source.open() as reader, sink.open() as writer:
                for batch in reader.read_batch():
                    if not self.running:
                        break

                    # 1. Update State (Process Micro-batch)
                    if batch:
                        self._update_state(batch)

                    # 2. Check Window Flush
                    if time.time() - self.last_flush_time >= self.window_seconds:
                        self._flush_window(writer)
        finally:
            self._end_run_loop()

    def _update_state(self, batch):
        """
        updates internal state aggregation on new messages
        Expected msg format: {"symbol": "AAPL", "price": 150.0}
        """
        for msg in batch:
            sym = msg.get("symbol")
            price = msg.get("price")
            if sym and price is not None:
                self.state[sym]["count"] += 1
                self.state[sym]["sum_price"] += float(price)
                self.metrics["total_processed"] += 1

    def _flush_window(self, writer):
        """
        Calculates averages and writes to DB, then resets state.
        """
        if not self.state:
            self.last_flush_time = time.time()
            return

        results = []
        timestamp = int(time.time())
        
        for sym, stats in self.state.items():
            if stats["count"] > 0:
                avg_price = stats["sum_price"] / stats["count"]
                results.append({
                    "time": timestamp,
                    "symbol": sym,
                    "avg_price": avg_price,
                    "count": stats["count"]
                })
        
        if results:
            print(f"[{self.name}] Flushing {len(results)} aggregates...")
            try:
                writer.write_batch(results)
            except Exception as e:
                print(f"[{self.name}] Error flushing state: {e}")
        
        self.metrics["last_flush_count"] = len(results)
        self.metrics["last_flush_time"] = timestamp

        # Reset Window
        self.state.clear()
        self.last_flush_time = time.time()

    def get_status(self) -> Dict[str, Any]:
        return {
            "running": self.running,
            "metrics": self.metrics,
            "current_buffer_size": len(self.state)
        }

    def stop(self):
        super().stop()
