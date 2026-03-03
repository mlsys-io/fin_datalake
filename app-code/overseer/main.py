"""
Overseer Entry Point.

Run with: python -m overseer.main
Or:       uv run python -m overseer.main

Starts the standalone MAPE-K control loop that runs forever,
continuously monitoring Ray, Kafka, Prefect, and other services.
"""

from __future__ import annotations

import asyncio
import logging
import sys


def main():
    # Configure structured logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)-20s | %(levelname)-5s | %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )

    logger = logging.getLogger("overseer")
    logger.info("=" * 60)
    logger.info("  System Overseer — AI Lakehouse Autonomic Controller")
    logger.info("=" * 60)

    from overseer.loop import Overseer

    overseer = Overseer()

    try:
        asyncio.run(overseer.run())
    except KeyboardInterrupt:
        logger.info("Overseer shut down by user (Ctrl+C)")
    except Exception as e:
        logger.critical(f"Overseer crashed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
