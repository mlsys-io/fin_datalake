"""Unit tests for KafkaSource."""
import pytest
from unittest.mock import MagicMock, patch


def test_kafka_source_config():
    """Test that config is stored correctly."""
    from etl.io.sources.kafka import KafkaSource
    
    source = KafkaSource(
        bootstrap_servers="localhost:9092",
        topics=["test-topic", "another-topic"],
        group_id="my-group"
    )
    
    assert source.bootstrap_servers == "localhost:9092"
    assert source.topics == ["test-topic", "another-topic"]
    assert source.group_id == "my-group"
    assert source.auto_offset_reset == "earliest"
    assert source.batch_size == 100


def test_kafka_source_validation():
    """Test that invalid config raises errors."""
    from etl.io.sources.kafka import KafkaSource
    
    # Empty topics should fail
    with pytest.raises(ValueError, match="At least one topic"):
        KafkaSource(
            bootstrap_servers="localhost:9092",
            topics=[]
        )
    
    # Invalid offset reset should fail
    with pytest.raises(ValueError, match="auto_offset_reset"):
        KafkaSource(
            bootstrap_servers="localhost:9092",
            topics=["test"],
            auto_offset_reset="invalid"
        )


def test_kafka_reader_read_batch():
    """Test batch reading with mocked Kafka consumer."""
    from etl.io.sources.kafka import KafkaSource
    
    source = KafkaSource(
        bootstrap_servers="localhost:9092",
        topics=["test-topic"]
    )
    
    with patch("etl.io.sources.kafka.Consumer") as MockConsumer:
        mock_consumer = MockConsumer.return_value
        
        # Create mock messages
        mock_msg1 = MagicMock()
        mock_msg1.error.return_value = None
        mock_msg1.key.return_value = b"key1"
        mock_msg1.value.return_value = b'{"id": 1}'
        mock_msg1.topic.return_value = "test-topic"
        mock_msg1.partition.return_value = 0
        mock_msg1.offset.return_value = 100
        mock_msg1.timestamp.return_value = (1, 1234567890)
        
        mock_msg2 = MagicMock()
        mock_msg2.error.return_value = None
        mock_msg2.key.return_value = None
        mock_msg2.value.return_value = b'{"id": 2}'
        mock_msg2.topic.return_value = "test-topic"
        mock_msg2.partition.return_value = 0
        mock_msg2.offset.return_value = 101
        mock_msg2.timestamp.return_value = (0, 0)  # No timestamp
        
        # Return messages then None to end batch
        mock_consumer.poll.side_effect = [mock_msg1, mock_msg2, None]
        
        with source.open() as reader:
            reader._running = False  # Stop after first batch
            batches = list(reader.read_batch())
        
        assert len(batches) == 1
        batch = batches[0]
        assert len(batch) == 2
        assert batch[0]["key"] == "key1"
        assert batch[0]["value"] == {"id": 1}
        assert batch[1]["timestamp"] is None
