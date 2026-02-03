"""
Monitoring Demo Pipeline

Starts a Ray Actor service and monitors it from Prefect.
Imports are inside flow/task for remote Ray execution.
"""
import os
import time
from prefect import flow, get_run_logger
from prefect.artifacts import create_markdown_artifact

# Read Ray cluster address from environment
RAY_ADDRESS = os.environ.get("RAY_ADDRESS", "auto")


@flow(name="Streaming Service with Monitoring")
def monitor_service_flow(duration_seconds: int = 60):
    """
    Starts a Ray Actor service and monitors it from Prefect.
    """
    # Heavy imports inside flow - executes when flow runs
    import ray
    from etl.services.processing.stateful_processor import StatefulProcessorService
    
    logger = get_run_logger()
    
    # 1. Start Ray (if not already)
    if not ray.is_initialized():
        ray.init(address=RAY_ADDRESS, ignore_reinit_error=True)

    # 2. Deploy the Service Actor
    logger.info("Deploying StatefulProcessorService...")
    
    # Example Config
    source_conf = {"url": "wss://ws.bitstamp.net", "read_timeout": 1.0}
    sink_conf = {
        "host": "localhost", "user": "admin", "password": "pwd", 
        "database": "mydb", "table_name": "crypto_windowed"
    }

    ProcessorActor = StatefulProcessorService(
        name="CryptoProcessor",
        source_config=source_conf,
        sink_config=sink_conf,
        window_seconds=5
    ).as_ray_actor(num_cpus=1)

    actor_handle = ProcessorActor.remote()
    
    # Start the processing loop in background
    logger.info("Starting processing loop in background...")
    actor_handle.run.remote()

    # 3. Monitoring Loop
    start_time = time.time()
    
    try:
        while time.time() - start_time < duration_seconds:
            # Poll status
            status = ray.get(actor_handle.get_status.remote())
            metrics = status.get("metrics", {})
            
            # A. Log to Console (History)
            logger.info(f"Service Status: {status}")
            
            # B. Publish Artifact (Live Dashboard)
            md_report = f"""
# 📡 Ray Service Live Dashboard
**Service**: CryptoProcessor
**Status**: {"🟢 Running" if status.get("running") else "🔴 Stopped"}

## 📊 Metrics
| Metric | Value |
|:--- |:--- |
| **Total Processed** | `{metrics.get('total_processed')}` |
| **Last Flush Count** | `{metrics.get('last_flush_count')}` |
| **Buffer Size** | `{status.get('current_buffer_size')}` |

*Last Updated: {time.strftime('%H:%M:%S')}*
            """
            create_markdown_artifact(
                key="service-status",
                markdown=md_report,
                description="Live metrics from Ray Actor"
            )
            
            time.sleep(5)
            
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    except ray.exceptions.RayActorError as e:
        logger.error(f"🚨 Ray Actor CRASHED: {e}")
        raise RuntimeError("Service Actor Died unexpectedly!") from e
    except Exception as e:
        logger.error(f"Monitoring Error: {e}")
        raise
    finally:
        logger.info("Stopping Service...")
        try:
            actor_handle.stop.remote()
        except Exception:
            pass
        logger.info("Service Stopped.")


if __name__ == "__main__":
    monitor_service_flow()
