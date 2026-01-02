"""
Centralized logging configuration using loguru.
Supports PII masking and Audit Context.
"""

import os
import sys
import re
from pathlib import Path
from loguru import logger

# Ensure logs directory exists
LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "app.log"

# Remove default handler
logger.remove()

# --- Compliance: PII Masking ---
# Regex patterns for sensitive data
EMAIL_PATTERN = r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"
SSN_PATTERN = r"\d{3}-\d{2}-\d{4}"
CREDIT_CARD_PATTERN = r"\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}"

def mask_pii(record):
    """
    Filter function to mask PII in log messages.
    Modifies the record["message"] in place before it is processed by sinks.
    """
    message = record["message"]

    # Mask Email
    if re.search(EMAIL_PATTERN, message):
        message = re.sub(EMAIL_PATTERN, "[MASKED_EMAIL]", message)

    # Mask SSN
    if re.search(SSN_PATTERN, message):
        message = re.sub(SSN_PATTERN, "[MASKED_SSN]", message)

    # Mask Credit Card
    if re.search(CREDIT_CARD_PATTERN, message):
        message = re.sub(CREDIT_CARD_PATTERN, "[MASKED_CC]", message)

    record["message"] = message
    return True

# --- Compliance: Audit Context ---
def audit_context(record):
    """
    Injects system user and context into the extra dict.
    """
    record["extra"]["user"] = os.getenv("USER", "system_user")
    record["extra"]["environment"] = os.getenv("APP_ENV", "development")
    return True

# Apply the context injector globally (using patch is one way, but configure is better for bind)
logger.configure(patcher=audit_context)

# Sink 1: Stderr (Console) - Human Readable
logger.add(
    sys.stderr,
    level=os.getenv("LOG_LEVEL", "INFO"),
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<magenta>{extra[user]}</magenta> | "  # Audit: Who
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    ),
    filter=mask_pii, # Apply PII masking
)

# Sink 2: File (JSON) - Machine Readable / Audit Log
logger.add(
    LOG_FILE,
    rotation="500 MB",
    retention="10 days",
    serialize=True,
    enqueue=True,
    level="DEBUG",
    filter=mask_pii, # Apply PII masking
)

# Export the configured logger
__all__ = ["logger"]
