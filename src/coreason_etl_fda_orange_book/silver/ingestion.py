# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Ingestion logic for Silver layer."""

from collections.abc import Iterator
from pathlib import Path

import dlt
from loguru import logger

from coreason_etl_fda_orange_book.silver.models import SilverExclusivity, SilverPatent, SilverProduct
from coreason_etl_fda_orange_book.silver.transform import transform_exclusivity, transform_patents, transform_products


@dlt.resource(name="FDA_ORANGE_BOOK_SILVER_PRODUCTS", write_disposition="replace", primary_key="coreason_id", columns=SilverProduct)
def silver_products_resource(files_map: dict[str, list[Path]]) -> Iterator[SilverProduct]:
    """
    DLT resource for Silver products.

    Args:
        files_map: A dictionary mapping logical file keys to lists of file paths.

    Yields:
        SilverProduct: Validated Silver product records.
    """
    if "products" not in files_map:
        logger.warning("No product files found for Silver layer.")
        return

    for file_path in files_map["products"]:
        logger.info(f"Processing {file_path} for Silver Products")
        filename = file_path.name.lower()
        hint = "RX"
        if "otc" in filename:
            hint = "OTC"
        elif "disc" in filename:
            hint = "DISCN"

        df = transform_products(file_path, marketing_status_hint=hint)
        if df.is_empty():
            continue

        for row in df.iter_rows(named=True):
            yield SilverProduct(**row)


@dlt.resource(
    name="FDA_ORANGE_BOOK_SILVER_PATENTS",
    write_disposition="replace",
    primary_key=["application_number", "product_number", "patent_number"],
    columns=SilverPatent,
)
def silver_patents_resource(files_map: dict[str, list[Path]]) -> Iterator[SilverPatent]:
    """
    DLT resource for Silver patents.

    Args:
        files_map: A dictionary mapping logical file keys to lists of file paths.

    Yields:
        SilverPatent: Validated Silver patent records.
    """
    if "patent" not in files_map:
        logger.warning("No patent files found for Silver layer.")
        return

    for file_path in files_map["patent"]:
        logger.info(f"Processing {file_path} for Silver Patents")
        df = transform_patents(file_path)
        if df.is_empty():
            continue

        for row in df.iter_rows(named=True):
            yield SilverPatent(**row)


@dlt.resource(
    name="FDA_ORANGE_BOOK_SILVER_EXCLUSIVITY",
    write_disposition="replace",
    primary_key=["application_number", "product_number", "exclusivity_code"],
    columns=SilverExclusivity,
)
def silver_exclusivity_resource(files_map: dict[str, list[Path]]) -> Iterator[SilverExclusivity]:
    """
    DLT resource for Silver exclusivity.

    Args:
        files_map: A dictionary mapping logical file keys to lists of file paths.

    Yields:
        SilverExclusivity: Validated Silver exclusivity records.
    """
    if "exclusivity" not in files_map:
        logger.warning("No exclusivity files found for Silver layer.")
        return

    for file_path in files_map["exclusivity"]:
        logger.info(f"Processing {file_path} for Silver Exclusivity")
        df = transform_exclusivity(file_path)
        if df.is_empty():
            continue

        for row in df.iter_rows(named=True):
            yield SilverExclusivity(**row)
