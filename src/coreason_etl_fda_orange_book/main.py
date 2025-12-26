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

import dlt
from loguru import logger

from coreason_etl_fda_orange_book.bronze.ingestion import bronze_resource
from coreason_etl_fda_orange_book.config import FdaConfig
from coreason_etl_fda_orange_book.gold.ingestion import gold_products_resource
from coreason_etl_fda_orange_book.silver.ingestion import (
    silver_exclusivity_resource,
    silver_patents_resource,
    silver_products_resource,
)
from coreason_etl_fda_orange_book.source import FdaOrangeBookSource


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


def run_pipeline(base_url: str, download_dir: Path) -> None:
    """
    Run the full Bronze -> Silver -> Gold pipeline.

    Args:
        base_url: The URL to download data from.
        download_dir: Local directory to store files.
    """
    # 1. Source (Download & Extract)
    logger.info("Step 1: Downloading and Extracting Source Data...")
    source = FdaOrangeBookSource(base_url=base_url)

    # Ensure directory exists
    download_dir.mkdir(parents=True, exist_ok=True)
    zip_path = download_dir / "orange_book.zip"

    # Download
    source.download_archive(zip_path)

    # Extract
    extracted_dir = download_dir / "extracted"
    extracted_dir.mkdir(exist_ok=True)
    extracted_files = source.extract_archive(zip_path, extracted_dir)

    # Map Files
    files_map = source.resolve_product_files(extracted_files)

    # 2. Bronze Layer
    logger.info("Step 2: Bronze Layer Ingestion...")
    pipeline = dlt.pipeline(
        pipeline_name="fda_orange_book",
        destination="postgresql",
        dataset_name="bronze",
        progress="log"
    )

    # We use 'run' with the resource.
    # Note: In a real CLI, we assume 'destination' is configured via secrets.toml or env vars.
    # If not configured, dlt might default or fail.
    # For now, we assume the user has set up credentials.

    bronze_info = pipeline.run(bronze_resource(files_map, source))
    logger.info(f"Bronze Load Info: {bronze_info}")

    # 3. Silver Layer
    logger.info("Step 3: Silver Layer Ingestion...")
    # Typically Silver is a separate schema or pipeline run.
    # We can reuse the pipeline object but change dataset or keep same schema?
    # Medallion often uses separate schemas: bronze, silver, gold.

    pipeline_silver = dlt.pipeline(
        pipeline_name="fda_orange_book",
        destination="postgresql",
        dataset_name="silver",
        progress="log"
    )

    silver_info = pipeline_silver.run([
        silver_products_resource(files_map),
        silver_patents_resource(files_map),
        silver_exclusivity_resource(files_map),
    ])
    logger.info(f"Silver Load Info: {silver_info}")

    # 4. Gold Layer
    logger.info("Step 4: Gold Layer Ingestion...")
    pipeline_gold = dlt.pipeline(
        pipeline_name="fda_orange_book",
        destination="postgresql",
        dataset_name="gold",
        progress="log"
    )

    gold_info = pipeline_gold.run(gold_products_resource(files_map))
    logger.info(f"Gold Load Info: {gold_info}")

    logger.info("Pipeline completed successfully.")


def main(args: list[str] | None = None) -> None:
    """Main entry point for the pipeline."""
    setup_logging()
    parsed_args = parse_args(args)

    logger.info("Starting FDA Orange Book ETL Pipeline")

    try:
        run_pipeline(parsed_args.base_url, parsed_args.download_dir)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":  # pragma: no cover
    main()
