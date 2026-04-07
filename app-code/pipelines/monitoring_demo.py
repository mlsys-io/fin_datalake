"""
Monitoring Demo Pipeline

Runs a lightweight ServiceTask on the remote Ray cluster and monitors it from
Prefect. This is intended as a reliable smoke test for the ServiceTask actor
lifecycle itself: deploy -> run -> poll status -> stop.
"""

import time

from prefect import flow, get_run_logger
from prefect.artifacts import create_markdown_artifact

from etl.core.base_service import ServiceTask
from etl.runtime import ensure_ray


class HeartbeatService(ServiceTask):
    """Minimal long-running service used to validate remote ServiceTask behavior."""

    def __init__(self, name: str | None = None, config: dict | None = None, **kwargs):
        super().__init__(name=name, config=config, **kwargs)
        cfg = config or {}
        self.tick_interval = float(cfg.get("tick_interval", 1.0))
        self.tick_count = 0
        self.last_tick_at = None

    def run(self):
        if not self._begin_run_loop():
            return

        try:
            while self.running:
                self.tick_count += 1
                self.last_tick_at = time.time()
                time.sleep(self.tick_interval)
        finally:
            self._end_run_loop()

    def get_status(self):
        return {
            "running": self.running,
            "tick_count": self.tick_count,
            "last_tick_at": self.last_tick_at,
            "tick_interval": self.tick_interval,
        }


@flow(name="Streaming Service with Monitoring")
def monitor_service_flow(duration_seconds: int = 20, tick_interval: float = 1.0):
    """
    Deploy and monitor a lightweight ServiceTask on the remote Ray cluster.
    """
    logger = get_run_logger()

    ray = ensure_ray()

    service_name = f"HeartbeatService-{int(time.time())}"
    logger.info("Deploying HeartbeatService to Ray as '%s'...", service_name)

    svc = HeartbeatService.deploy(
        name=service_name,
        config={"tick_interval": tick_interval},
        num_cpus=0.1,
        max_concurrency=2,
    )

    logger.info("Starting service loop in background...")
    svc.async_run()

    first_status = None
    last_status = None
    start_time = time.time()

    try:
        while time.time() - start_time < duration_seconds:
            status = svc.get_status()
            if first_status is None:
                first_status = status
            last_status = status

            logger.info("Service Status: %s", status)

            md_report = f"""
# ServiceTask Smoke Test
**Service**: `{service_name}`
**Status**: {"Running" if status.get("running") else "Stopped"}

## Metrics
| Metric | Value |
|:--- |:--- |
| **Tick Count** | `{status.get('tick_count')}` |
| **Tick Interval** | `{status.get('tick_interval')}` |
| **Last Tick At** | `{status.get('last_tick_at')}` |

*Last Updated: {time.strftime('%H:%M:%S')}*
            """
            create_markdown_artifact(
                key="service-status",
                markdown=md_report,
                description="Live metrics from ServiceTask smoke test",
            )

            time.sleep(2)

        if not last_status or not last_status.get("running"):
            raise RuntimeError("Service never reported a running state.")
        if last_status.get("tick_count", 0) < 2:
            raise RuntimeError(f"Service tick_count too low: {last_status.get('tick_count')}")
        if first_status and last_status["tick_count"] <= first_status.get("tick_count", -1):
            raise RuntimeError("Service status did not advance while monitoring.")

        logger.info("ServiceTask smoke test passed.")

    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    except ray.exceptions.RayActorError as e:
        logger.error("Ray Actor crashed: %s", e)
        raise RuntimeError("Service actor died unexpectedly.") from e
    finally:
        logger.info("Stopping service...")
        try:
            svc.stop()
        except Exception:
            pass
        logger.info("Service stopped.")


if __name__ == "__main__":
    monitor_service_flow()
