# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Ingestion logic for the Silver layer."""

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import dlt
from loguru import logger

from coreason_etl_fda_orange_book.silver.transform import (
    transform_exclusivity,
    transform_patents,
    transform_products,
)


@dlt.resource(name="silver_products", write_disposition="merge", primary_key="coreason_id")
def silver_products_resource(
    files_map: dict[str, list[Path]],
) -> Iterator[dict[str, Any]]:
    """
    DLT resource for Silver Products.

    Args:
        files_map: Dictionary mapping roles to file paths.

    Yields:
        Dictionary records for the Silver Products table.
    """
    if "products" not in files_map:
        logger.warning("No product files found in files_map for Silver layer.")
        return

    for file_path in files_map["products"]:
        logger.info(f"Processing {file_path} for Silver Products")
        # Determine marketing status hint from filename if needed
        # e.g., if file is "rx.txt", hint="RX"
        filename = file_path.name.lower()
        hint = "RX"
        if "otc" in filename:
            hint = "OTC"
        elif "disc" in filename:
            hint = "DISCN"

        df = transform_products(file_path, marketing_status_hint=hint)
        if df.is_empty():
            continue

        # Convert to list of dicts for DLT
        # rows(named=True) returns iterator of dicts
        yield from df.iter_rows(named=True)


@dlt.resource(
    name="silver_patents",
    write_disposition="merge",
    primary_key=[
        "application_number",
        "product_number",
        "patent_number",
        "patent_use_code",
    ],
)
def silver_patents_resource(
    files_map: dict[str, list[Path]],
) -> Iterator[dict[str, Any]]:
    """
    DLT resource for Silver Patents.

    Args:
        files_map: Dictionary mapping roles to file paths.

    Yields:
        Dictionary records for the Silver Patents table.
    """
    if "patent" not in files_map:
        logger.warning("No patent files found in files_map for Silver layer.")
        return

    for file_path in files_map["patent"]:
        logger.info(f"Processing {file_path} for Silver Patents")
        df = transform_patents(file_path)
        if df.is_empty():
            continue
        yield from df.iter_rows(named=True)


@dlt.resource(
    name="silver_exclusivity",
    write_disposition="merge",
    primary_key=[
        "application_number",
        "product_number",
        "exclusivity_code",
    ],
)
def silver_exclusivity_resource(
    files_map: dict[str, list[Path]],
) -> Iterator[dict[str, Any]]:
    """
    DLT resource for Silver Exclusivity.

    Args:
        files_map: Dictionary mapping roles to file paths.

    Yields:
        Dictionary records for the Silver Exclusivity table.
    """
    if "exclusivity" not in files_map:
        logger.warning("No exclusivity files found in files_map for Silver layer.")
        return

    for file_path in files_map["exclusivity"]:
        logger.info(f"Processing {file_path} for Silver Exclusivity")
        df = transform_exclusivity(file_path)
        if df.is_empty():
            continue
        yield from df.iter_rows(named=True)
