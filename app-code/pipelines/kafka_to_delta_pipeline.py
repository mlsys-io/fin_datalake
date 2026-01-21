"""
Example pipeline demonstrating KafkaSource for event-driven ingestion.

This pipeline:
1. Consumes events from a Kafka topic
2. Transforms the data (extracts relevant fields)
3. Writes to Delta Lake for historical storage

Usage:
    python -m pipelines.kafka_to_delta_pipeline
    
Requires:
    - Kafka broker running at localhost:9092
    - Topic 'events' created
"""
from typing import List, Dict, Any
import pandas as pd
from prefect import flow, task
from prefect_ray.task_runners import RayTaskRunner

from etl.io.sources.kafka import KafkaSource
from etl.io.sinks.delta_lake import DeltaLakeSink


@task(name="Consume Kafka Events")
def consume_events(source: KafkaSource, max_batches: int = 10) -> List[Dict]:
    """
    Consume events from Kafka until max_batches reached.
    In production, this would run continuously in a ServiceTask.
    """
    all_events = []
    batch_count = 0
    
    with source.open() as reader:
        for batch in reader.read_batch():
            print(f"[Kafka] Received batch of {len(batch)} events")
            all_events.extend(batch)
            batch_count += 1
            
            if batch_count >= max_batches:
                reader.stop()
                break
    
    print(f"[Kafka] Total events consumed: {len(all_events)}")
    return all_events


@task(name="Transform Events")
def transform_events(events: List[Dict]) -> pd.DataFrame:
    """
    Transform raw Kafka events into a structured DataFrame.
    Extracts the 'value' field which contains the actual event data.
    """
    records = []
    for event in events:
        value = event.get("value", {})
        if isinstance(value, dict):
            record = {
                "kafka_topic": event.get("topic"),
                "kafka_partition": event.get("partition"),
                "kafka_offset": event.get("offset"),
                "kafka_timestamp": event.get("timestamp"),
                **value  # Flatten event data
            }
            records.append(record)
    
    df = pd.DataFrame(records)
    print(f"[Transform] Created DataFrame with {len(df)} rows, columns: {list(df.columns)}")
    return df


@task(name="Write to Delta Lake")
def write_to_delta(df: pd.DataFrame, sink: DeltaLakeSink):
    """Write the transformed data to Delta Lake."""
    if df.empty:
        print("[Delta] No data to write")
        return
    
    with sink.open() as writer:
        writer.write_batch(df)
    
    print(f"[Delta] Wrote {len(df)} rows to {sink.uri}")


@flow(name="Kafka to Delta Pipeline", task_runner=RayTaskRunner)
def kafka_to_delta_flow(
    kafka_bootstrap: str = "localhost:9092",
    kafka_topic: str = "events",
    delta_uri: str = "s3://delta-lake/kafka-events",
    max_batches: int = 10
):
    """
    End-to-end pipeline: Kafka -> Transform -> Delta Lake
    
    Args:
        kafka_bootstrap: Kafka broker address
        kafka_topic: Topic to consume from
        delta_uri: Delta Lake output path
        max_batches: Max number of batches to consume (for demo)
    """
    # Configure Source
    source = KafkaSource(
        bootstrap_servers=kafka_bootstrap,
        topics=[kafka_topic],
        group_id="etl-delta-consumer",
        batch_size=100,
        poll_timeout=2.0
    )
    
    # Configure Sink
    sink = DeltaLakeSink(
        uri=delta_uri,
        mode="append"
    )
    
    # Execute Pipeline
    raw_events = consume_events(source, max_batches)
    transformed_df = transform_events(raw_events)
    write_to_delta(transformed_df, sink)


if __name__ == "__main__":
    # Example local run
    kafka_to_delta_flow(
        kafka_bootstrap="localhost:9092",
        kafka_topic="test-events",
        delta_uri="tmp/delta/kafka_events",
        max_batches=5
    )
