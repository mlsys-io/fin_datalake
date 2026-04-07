"""
Overseer Entry Point.

Run with: python -m overseer.main
Or:       uv run python -m overseer.main

Starts the standalone MAPE-K control loop that runs forever,
continuously monitoring Ray, Kafka, Prefect, and other services.
"""

from __future__ import annotations

import asyncio
import os
import sys


def main():
    # Use the unified logging framework
    from etl.utils.logging import setup_logging
    from loguru import logger

    setup_logging(component="overseer")

    logger.info("=" * 60)
    logger.info("  System Overseer — AI Lakehouse Autonomic Controller")
    logger.info("=" * 60)

    from overseer.loop import Overseer

    config_path = os.environ.get("OVERSEER_CONFIG_PATH")
    if config_path:
        logger.info(f"Using Overseer config: {config_path}")

    overseer = Overseer(config_path=config_path)

    try:
        asyncio.run(overseer.run())
    except KeyboardInterrupt:
        logger.info("Overseer shut down by user (Ctrl+C)")
    except Exception as e:
        logger.critical(f"Overseer crashed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
