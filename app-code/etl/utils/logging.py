"""
Centralized logging configuration for the ETL framework.

Provides a single ``setup_logging()`` function that configures loguru with:
  - Console output (stderr, colored)
  - Optional persistent storage to TimescaleDB via ``TimescaleLogSink``

Every component should call this once at initialization::

    from etl.utils.logging import setup_logging
    setup_logging(component="agent")

All subsequent ``from loguru import logger; logger.info(...)`` calls will
then go to both console AND TimescaleDB automatically.
"""
from __future__ import annotations

import sys
from loguru import logger

# Track which components have been set up to avoid duplicate sinks
_initialized_components: set[str] = set()


def setup_logging(
    level: str = "INFO",
    component: str = "etl",
    enable_db_sink: bool = True,
    format_string: str | None = None,
) -> None:
    """
    Configure logging for a specific component.

    Idempotent — calling multiple times with the same component is safe;
    sinks are only added once per component.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        component: Identifies this component in the database
                   (e.g. 'agent', 'hub', 'context', 'overseer', 'gateway')
        enable_db_sink: If True, add TimescaleDB sink for persistent logging.
                        Set to False for local development or testing.
        format_string: Custom format string. Uses default if None.
    """
    if component in _initialized_components:
        return  # Already set up — avoid duplicate sinks

    # Only remove defaults on first call
    if not _initialized_components:
        logger.remove()

        # Console handler (stderr, colored)
        if format_string is None:
            format_string = (
                "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                "<level>{level: <8}</level> | "
                "<cyan>{name}</cyan>:<cyan>{function}</cyan> | "
                "<level>{message}</level>"
            )

        logger.add(
            sys.stderr,
            level=level,
            format=format_string,
            colorize=True,
        )

    # Add TimescaleDB sink for this component
    if enable_db_sink:
        try:
            from etl.utils.log_sink import TimescaleLogSink, is_timescale_logging_configured

            if is_timescale_logging_configured():
                sink = TimescaleLogSink(component=component)
                logger.add(
                    sink,
                    level=level,
                    format="{message}",  # Sink extracts structured fields from record
                    serialize=False,
                )
        except Exception as e:
            # Never crash because of logging setup
            print(
                f"[setup_logging] DB sink init failed for '{component}': {e}",
                file=sys.stderr,
            )

    _initialized_components.add(component)
