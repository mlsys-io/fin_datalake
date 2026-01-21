"""
Kafka Source for event-driven streaming ingestion.
Uses confluent-kafka for high-performance Kafka consumption.
"""
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any, Iterator, Callable
from loguru import logger

from etl.io.base import DataSource, DataReader


@dataclass
class KafkaSource(DataSource):
    """
    Configuration for Kafka consumer.
    
    Example:
        source = KafkaSource(
            bootstrap_servers="localhost:9092",
            topics=["events"],
            group_id="my-consumer-group"
        )
        
        with source.open() as reader:
            for batch in reader.read_batch():
                process(batch)
    """
    REQUIRED_DEPENDENCIES = ["confluent-kafka"]
    
    bootstrap_servers: str
    topics: List[str]
    group_id: str = "etl-consumer"
    auto_offset_reset: str = "earliest"  # "earliest" or "latest"
    batch_size: int = 100
    poll_timeout: float = 1.0
    consumer_config: Dict[str, Any] = field(default_factory=dict)
    value_deserializer: Optional[Callable[[bytes], Any]] = None
    
    def __post_init__(self):
        """Validate configuration."""
        if not self.topics:
            raise ValueError("At least one topic must be specified")
        if self.auto_offset_reset not in ["earliest", "latest"]:
            raise ValueError("auto_offset_reset must be 'earliest' or 'latest'")
    
    def open(self) -> 'KafkaReader':
        return KafkaReader(self)


class KafkaReader(DataReader):
    """
    Runtime Kafka consumer with micro-batching.
    
    Consumes messages from Kafka topics and yields them in batches
    for efficient processing in Ray/Prefect pipelines.
    """
    
    def __init__(self, source: KafkaSource):
        self.source = source
        self._consumer = None
        self._running = True
    
    def _connect(self):
        if self._consumer:
            return
        
        try:
            from confluent_kafka import Consumer
        except ImportError:
            raise ImportError(
                "KafkaSource requires 'confluent-kafka'. "
                "Install with: pip install confluent-kafka"
            )
        
        config = {
            'bootstrap.servers': self.source.bootstrap_servers,
            'group.id': self.source.group_id,
            'auto.offset.reset': self.source.auto_offset_reset,
            'enable.auto.commit': True,
            **self.source.consumer_config
        }
        
        self._consumer = Consumer(config)
        self._consumer.subscribe(self.source.topics)
        logger.info(f"Subscribed to Kafka topics: {self.source.topics}")
    
    def read_batch(self) -> Iterator[List[Dict[str, Any]]]:
        """
        Polls Kafka and yields batches of messages.
        
        Each message is returned as a dict with:
        - key: Message key (bytes or None)
        - value: Deserialized message value
        - topic: Source topic
        - partition: Partition number
        - offset: Message offset
        - timestamp: Message timestamp (if available)
        
        This is an infinite generator intended for streaming pipelines.
        Use stop() to gracefully terminate.
        """
        self._connect()
        
        while self._running:
            batch = []
            
            for _ in range(self.source.batch_size):
                msg = self._consumer.poll(self.source.poll_timeout)
                
                if msg is None:
                    # No more messages in poll window
                    break
                    
                if msg.error():
                    from confluent_kafka import KafkaError
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition, not an error
                        continue
                    logger.warning(f"Kafka consumer error: {msg.error()}")
                    continue
                
                # Deserialize value
                value = msg.value()
                if self.source.value_deserializer:
                    try:
                        value = self.source.value_deserializer(value)
                    except Exception as e:
                        logger.warning(f"Failed to deserialize message: {e}")
                        continue
                elif isinstance(value, bytes):
                    # Default: try JSON, fallback to string
                    try:
                        import json
                        value = json.loads(value.decode('utf-8'))
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        value = value.decode('utf-8', errors='replace')
                
                batch.append({
                    'key': msg.key().decode('utf-8') if msg.key() else None,
                    'value': value,
                    'topic': msg.topic(),
                    'partition': msg.partition(),
                    'offset': msg.offset(),
                    'timestamp': msg.timestamp()[1] if msg.timestamp()[0] != 0 else None
                })
            
            if batch:
                yield batch
            elif not self._running:
                # Exit cleanly if stopped
                break
    
    def stop(self):
        """Signal the reader to stop consuming."""
        self._running = False
        logger.info("KafkaReader stop requested")
    
    def close(self):
        """Close the Kafka consumer connection."""
        self._running = False
        if self._consumer:
            try:
                self._consumer.close()
                logger.info("Kafka consumer closed")
            except Exception as e:
                logger.warning(f"Error closing Kafka consumer: {e}")
            finally:
                self._consumer = None
