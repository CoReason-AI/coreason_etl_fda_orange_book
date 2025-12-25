# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Entry point for the FDA Orange Book ETL pipeline."""

import argparse
import os
import sys
from pathlib import Path

from loguru import logger

from coreason_etl_fda_orange_book.config import FdaConfig


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="FDA Orange Book ETL Pipeline")
    parser.add_argument(
        "--base-url",
        type=str,
        default=FdaConfig.DEFAULT_BASE_URL,
        help="Base URL for the FDA Orange Book ZIP download",
    )
    parser.add_argument(
        "--download-dir",
        type=Path,
        default=FdaConfig.DEFAULT_DOWNLOAD_DIR,
        help="Directory where the source ZIP will be downloaded and extracted",
    )
    return parser.parse_args(args)


def setup_logging() -> None:
    """Configure logging based on environment variables."""
    logger.remove()
    logger.add(sys.stderr, level=os.getenv("LOG_LEVEL", "INFO"))


def main(args: list[str] | None = None) -> None:
    """Main entry point for the pipeline."""
    setup_logging()
    parsed_args = parse_args(args)

    logger.info("Starting FDA Orange Book ETL Pipeline")
    logger.info(f"Using Base URL: {parsed_args.base_url}")
    logger.info(f"Download Directory: {parsed_args.download_dir}")

    # Ensure download directory exists (rudimentary check, source logic might handle it too)
    # But since we are passing it to the source, the source might expect it to exist or create it.
    # We'll leave creation to the specific logic or here.
    # For now, we just parse it.

    # Placeholder for future logic
    logger.info("Pipeline initialized successfully.")


if __name__ == "__main__":  # pragma: no cover
    main()
