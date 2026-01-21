"""
Centralized logging configuration using loguru.
"""
from loguru import logger
import sys


def setup_logging(level="INFO", format_string=None):
    """
    Configure logging for the ETL framework.
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_string: Custom format string, uses default if None
    
    Returns:
        Configured logger instance
    """
    # Remove default handler
    logger.remove()
    
    # Use custom format or default
    if format_string is None:
        format_string = (
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan> | "
            "<level>{message}</level>"
        )
    
    # Add stderr handler
    logger.add(
        sys.stderr,
        level=level,
        format=format_string,
        colorize=True
    )
    
    return logger


# Initialize default logger
setup_logging()
