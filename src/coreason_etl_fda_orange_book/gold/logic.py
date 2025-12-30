# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Business logic for the Gold layer."""

import polars as pl

from coreason_etl_fda_orange_book.utils.logger import logger


def create_gold_view(
    products_df: pl.DataFrame,
    patents_df: pl.DataFrame,
    exclusivity_df: pl.DataFrame,
    include_discontinued: bool = False,
) -> pl.DataFrame:
    """
    Create the denormalized Gold view.

    Args:
        products_df: Silver Products DataFrame.
        patents_df: Silver Patents DataFrame.
        exclusivity_df: Silver Exclusivity DataFrame.
        include_discontinued: Whether to include DISCN status products.

    Returns:
        Joined and enriched DataFrame.
    """
    logger.info("Creating Gold view")

    if products_df.is_empty():
        return pl.DataFrame()

    # 1. Filter Discontinued
    # 'marketing_status' -> 'DISCN' usually.
    # Logic: "Exclude discontinued products unless specifically requested (Flag: is_active)"
    # Assumption: active = not DISCN
    if not include_discontinued:
        products_df = products_df.filter(pl.col("marketing_status") != "DISCN")

    # 2. Enrichment: Vectorization Prep
    # Concatenate trade_name + ingredient
    products_df = products_df.with_columns(
        (pl.col("trade_name") + " " + pl.col("ingredient")).alias("search_vector_text")
    )

    # 3. Joins
    # Keys: application_number, product_number
    # Left Join Products -> Patents
    # Note: Patents DF might not have all columns if empty.

    joined_df = products_df

    # Patents Join
    if not patents_df.is_empty():
        # Select relevant patent columns to join (exclude duplicate keys if needed, but 'on' handles it)
        # Note: join keys must match type (String).
        joined_df = joined_df.join(patents_df, on=["application_number", "product_number"], how="left", coalesce=True)
    else:
        # Add null columns for patent schema consistency?
        # Polars lazy evaluation might handle this if we typed it, but for eager:
        # We can let the consumer model handle missing cols or add them here.
        # Let's add them as nulls to ensure consistent schema.
        patent_cols = [
            "patent_number",
            "patent_expiry_date",
            "is_drug_substance",
            "is_drug_product",
            "patent_use_code",
            "is_delisted",
            "submission_date",
        ]
        joined_df = joined_df.with_columns([pl.lit(None).alias(c) for c in patent_cols])

    # Exclusivity Join
    if not exclusivity_df.is_empty():
        joined_df = joined_df.join(
            exclusivity_df, on=["application_number", "product_number"], how="left", coalesce=True
        )
    else:
        excl_cols = ["exclusivity_code", "exclusivity_end_date"]
        joined_df = joined_df.with_columns([pl.lit(None).alias(c) for c in excl_cols])

    return joined_df
