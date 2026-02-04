"""
Kafka to Delta Lake Pipeline

Demonstrates consuming events from Kafka and writing to Delta Lake.
Imports are inside task methods for remote Ray execution.

Usage:
    python -m pipelines.kafka_to_delta_pipeline
    
Requires:
    - Kafka broker running
    - Topic 'events' created
"""
from typing import List, Dict, Any
from prefect import flow, task
from prefect_ray.task_runners import RayTaskRunner

# Load config (auto-loads .env via python-dotenv)
from etl.config import config


@task(name="Consume Kafka Events")
def consume_events(
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    max_batches: int = 10
) -> List[Dict]:
    """
    Consume events from Kafka until max_batches reached.
    """
    # Heavy imports inside task - executes on Ray worker
    from etl.io.sources.kafka import KafkaSource
    
    source = KafkaSource(
        bootstrap_servers=bootstrap_servers,
        topics=[topic],
        group_id=group_id,
        batch_size=100,
        poll_timeout=2.0
    )
    
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
def transform_events(events: List[Dict]) -> Any:
    """
    Transform raw Kafka events into a structured DataFrame.
    """
    # Heavy imports inside task - executes on Ray worker
    import pandas as pd
    
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
def write_to_delta(df: Any, delta_uri: str, mode: str = "append"):
    """
    Write the transformed data to Delta Lake.
    
    NOTE: This task has Tokio/Ray conflict. Use DeltaLakeWriteTask.local() 
    in the flow instead for reliable execution.
    """
    # Heavy imports inside task - executes on Ray worker
    import pandas as pd
    from etl.io.sinks.delta_lake import DeltaLakeSink
    
    if isinstance(df, pd.DataFrame) and df.empty:
        print("[Delta] No data to write")
        return
    
    sink = DeltaLakeSink(uri=delta_uri, mode=mode)
    
    with sink.open() as writer:
        writer.write_batch(df)
    
    print(f"[Delta] Wrote {len(df)} rows to {delta_uri}")


@flow(name="Kafka to Delta Pipeline", task_runner=RayTaskRunner(address=config.RAY_ADDRESS))
def kafka_to_delta_flow(
    kafka_bootstrap: str = "",
    kafka_topic: str = "ohlc-events",
    delta_uri: str = "s3://delta-lake/bronze/kafka-events",
    max_batches: int = 5  # Limit for demo (each batch ~100 msgs)
):
    """
    End-to-end pipeline: Kafka -> Transform -> Delta Lake
    
    Pipeline structure:
    - Kafka Consume: Runs on Ray (distributed)
    - Transform: Runs on Ray (distributed)
    - Delta Write: Runs locally (avoids Tokio/Ray conflict)
    
    Args:
        kafka_bootstrap: Kafka broker address (defaults to config.KAFKA_BOOTSTRAP_SERVERS)
        kafka_topic: Topic to consume from
        delta_uri: Delta Lake output path
        max_batches: Max number of batches to consume (for demo)
    """
    from etl.io.tasks.delta_lake_write_task import DeltaLakeWriteTask
    
    # Use config if not specified
    if not kafka_bootstrap:
        kafka_bootstrap = config.KAFKA_BOOTSTRAP_SERVERS
    
    print(f"[flow] Starting Kafka to Delta Pipeline")
    print(f"[flow] Kafka: {kafka_bootstrap}, Topic: {kafka_topic}")
    print(f"[flow] Output: {delta_uri}")
    
    # Execute: Consume and Transform on Ray (distributed)
    raw_events = consume_events.submit(
        kafka_bootstrap, 
        kafka_topic, 
        "etl-delta-consumer",
        max_batches
    )
    transformed_df = transform_events.submit(raw_events)
    
    # Wait for Ray results
    df = transformed_df.result()
    
    if df is None or (hasattr(df, 'empty') and df.empty):
        print("[flow] No data to write")
        return "No data"
    
    print(f"[flow] Got {len(df)} rows, writing to Delta Lake...")
    
    # Execute: Write locally (avoids Tokio crash)
    write_task = DeltaLakeWriteTask(
        name="Write Kafka to Delta",
        uri=delta_uri,
        mode="append"  # Append for streaming data
    )
    result = write_task.local(df)
    
    print(f"[flow] Pipeline complete: {result}")
    return result


if __name__ == "__main__":
    # Run with config values
    kafka_to_delta_flow(
        kafka_topic="ohlc-events",
        delta_uri="s3://delta-lake/bronze/kafka-events",
        max_batches=1
    )
