"""Centralized logging configuration using loguru."""

import os
import sys
from pathlib import Path

from loguru import logger

# Ensure logs directory exists
LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "app.log"

# Remove default handler
logger.remove()

# Sink 1: Stderr (Console)
logger.add(
    sys.stderr,
    level=os.getenv("LOG_LEVEL", "INFO"),
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    ),
)

# Sink 2: File (JSON)
logger.add(
    LOG_FILE,
    rotation="500 MB",
    retention="10 days",
    serialize=True,
    enqueue=True,
    level="DEBUG",  # Capture more details in file
)

# Export the configured logger
__all__ = ["logger"]
