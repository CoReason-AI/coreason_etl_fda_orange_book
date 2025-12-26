# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Ingestion logic for the Gold layer."""

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import dlt
import polars as pl

from coreason_etl_fda_orange_book.gold.logic import create_gold_view
from coreason_etl_fda_orange_book.silver.transform import (
    transform_exclusivity,
    transform_patents,
    transform_products,
)


@dlt.resource(name="gold_enriched_products", write_disposition="replace")
def gold_products_resource(
    files_map: dict[str, list[Path]],
) -> Iterator[dict[str, Any]]:
    """
    DLT resource for Gold Enriched Products.

    This resource orchestrates the reading of all Silver inputs (via transform functions)
    and applies the Gold logic to yield the final denormalized view.

    Args:
        files_map: Dictionary mapping roles to file paths.

    Yields:
        Dictionary records for the Gold table.
    """
    # 1. Load Silver Dataframes
    # We re-read/transform here since we are simulating the flow from local files.
    # In a real warehouse, we might query the Silver tables.
    # Given the constraint to use Polars -> Postgres, and the context of this tool,
    # we rebuild from source files using the Silver transforms.

    if "products" not in files_map:
        return

    # Aggregate all product files
    prod_dfs = []
    for f in files_map["products"]:
        hint = "RX"
        if "otc" in f.name.lower():
            hint = "OTC"
        elif "disc" in f.name.lower():
            hint = "DISCN"
        df = transform_products(f, marketing_status_hint=hint)
        if not df.is_empty():
            prod_dfs.append(df)

    if not prod_dfs:
        return
    products_df = pl.concat(prod_dfs)

    # Patents
    pat_dfs = []
    if "patent" in files_map:
        for f in files_map["patent"]:
            df = transform_patents(f)
            if not df.is_empty():
                pat_dfs.append(df)
    patents_df = pl.concat(pat_dfs) if pat_dfs else pl.DataFrame()

    # Exclusivity
    exc_dfs = []
    if "exclusivity" in files_map:
        for f in files_map["exclusivity"]:
            df = transform_exclusivity(f)
            if not df.is_empty():
                exc_dfs.append(df)
    exclusivity_df = pl.concat(exc_dfs) if exc_dfs else pl.DataFrame()

    # 2. Apply Gold Logic
    gold_df = create_gold_view(products_df, patents_df, exclusivity_df)

    # 3. Yield
    yield from gold_df.iter_rows(named=True)
