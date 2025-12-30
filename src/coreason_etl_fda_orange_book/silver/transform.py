# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Transformation logic for the Silver layer using Polars."""

import uuid
from pathlib import Path
from typing import Optional

import polars as pl

from coreason_etl_fda_orange_book.config import FdaConfig
from coreason_etl_fda_orange_book.utils.logger import logger


def _generate_coreason_id(source_id: str) -> str:
    """
    Generate a UUID5 coreason_id from the source_id.

    Args:
        source_id: The unique source identifier string.

    Returns:
        String representation of the UUID5.
    """
    namespace = uuid.uuid5(uuid.NAMESPACE_DNS, FdaConfig.NAMESPACE_FDA)
    return str(uuid.uuid5(namespace, source_id))


def _parse_fda_date(date_str: Optional[str]) -> Optional[str]:
    """
    Parse FDA date format (e.g., 'Jan 1, 1982') to ISO 8601 (YYYY-MM-DD).

    Args:
        date_str: Date string from the source.

    Returns:
        ISO 8601 formatted date string or None if invalid/approved prior.
    """
    if not date_str or "Approved prior to" in date_str:
        return None

    try:
        # Polars str.to_date with format might be cleaner, but for row-wise UDF map_elements:
        from datetime import datetime

        dt = datetime.strptime(date_str.strip(), "%b %d, %Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def _clean_read_csv(file_path: Path) -> pl.DataFrame:
    """Helper to read CSV safely."""
    try:
        df = pl.read_csv(
            file_path,
            separator=FdaConfig.DELIMITER,
            has_header=True,
            infer_schema_length=10000,
            encoding="utf8-lossy",
            truncate_ragged_lines=True,
        )
        # Normalize column names
        return df.rename({col: col.strip() for col in df.columns})
    except Exception as e:
        logger.error(f"Failed to read CSV {file_path}: {e}")
        return pl.DataFrame()


def transform_products(file_path: Path, marketing_status_hint: str = "RX") -> pl.DataFrame:
    """
    Transform raw product files into the Silver schema.

    Args:
        file_path: Path to the raw product file.
        marketing_status_hint: Fallback marketing status (RX/OTC/DISCN) if not in columns.

    Returns:
        Polars DataFrame matching the SilverProduct schema.
    """
    logger.info(f"Transforming product file: {file_path}")
    df = _clean_read_csv(file_path)

    if df.is_empty():
        return df

    # Global string cleaning for values
    df = df.with_columns(pl.all().cast(pl.String).str.strip_chars())

    col_map = {c.lower(): c for c in df.columns}
    has_type_col = "type" in col_map

    def safe_col(name: str) -> pl.Expr:
        real_name = col_map.get(name.lower())
        if real_name:
            return pl.col(real_name)
        return pl.lit(None).cast(pl.String)

    # Cast integer columns to string before padding if they were inferred as int
    def safe_col_str(name: str) -> pl.Expr:
        real_name = col_map.get(name.lower())
        if real_name:
            return pl.col(real_name).cast(pl.String)
        return pl.lit(None).cast(pl.String)

    df_silver = df.select(
        [
            safe_col("Ingredient").alias("ingredient"),
            safe_col("Trade_Name").alias("trade_name"),
            safe_col("Applicant").alias("applicant_short"),
            safe_col("Strength").alias("strength"),
            safe_col_str("Appl_No").str.pad_start(6, "0").alias("application_number"),
            safe_col_str("Product_No").str.pad_start(3, "0").alias("product_number"),
            safe_col("TE_Code").alias("te_code"),
            safe_col("Approval_Date")
            .map_elements(lambda x: _parse_fda_date(x), return_dtype=pl.String)
            .str.to_date("%Y-%m-%d", strict=False)
            .alias("approval_date"),
            pl.when(safe_col("RLD").str.to_uppercase() == "NO")
            .then(False)
            .when(safe_col("RLD").str.to_uppercase() == "YES")
            .then(True)
            .otherwise(False)
            .alias("is_rld"),
            (pl.col(col_map["type"]) if has_type_col else pl.lit(marketing_status_hint)).alias("marketing_status"),
        ]
    )

    df_silver = df_silver.with_columns(
        (pl.col("application_number") + pl.col("product_number") + pl.col("marketing_status")).alias("source_id")
    )

    df_silver = df_silver.with_columns(
        pl.col("source_id").map_elements(_generate_coreason_id, return_dtype=pl.String).alias("coreason_id")
    )

    return df_silver.filter(pl.col("source_id").is_not_null() & (pl.col("source_id") != ""))


def transform_patents(file_path: Path) -> pl.DataFrame:
    """
    Transform raw patent files into the Silver schema.
    """
    logger.info(f"Transforming patent file: {file_path}")
    df = _clean_read_csv(file_path)

    if df.is_empty():
        return df

    # Global string cleaning for values
    df = df.with_columns(pl.all().cast(pl.String).str.strip_chars())

    col_map = {c.lower(): c for c in df.columns}

    def safe_col(name: str) -> pl.Expr:
        real_name = col_map.get(name.lower())
        if real_name:
            return pl.col(real_name)
        return pl.lit(None).cast(pl.String)

    def safe_col_str(name: str) -> pl.Expr:
        real_name = col_map.get(name.lower())
        if real_name:
            return pl.col(real_name).cast(pl.String)
        return pl.lit(None).cast(pl.String)

    def bool_flag(col_name: str) -> pl.Expr:
        return pl.when(safe_col(col_name).str.to_uppercase() == "Y").then(True).otherwise(False)

    df_silver = df.select(
        [
            safe_col_str("Appl_No").str.pad_start(6, "0").alias("application_number"),
            safe_col_str("Product_No").str.pad_start(3, "0").alias("product_number"),
            safe_col_str("Patent_No").alias("patent_number"),
            safe_col("Patent_Expire_Date_Text")
            .map_elements(lambda x: _parse_fda_date(x), return_dtype=pl.String)
            .str.to_date("%Y-%m-%d", strict=False)
            .alias("patent_expiry_date"),
            bool_flag("Drug_Substance_Flag").alias("is_drug_substance"),
            bool_flag("Drug_Product_Flag").alias("is_drug_product"),
            safe_col("Patent_Use_Code").alias("patent_use_code"),
            bool_flag("Delist_Flag").alias("is_delisted"),
            # Submission Date is not always present or explicitly named consistently, trying best guess
            safe_col("Submission_Date")
            .map_elements(lambda x: _parse_fda_date(x), return_dtype=pl.String)
            .str.to_date("%Y-%m-%d", strict=False)
            .alias("submission_date"),
        ]
    )

    return df_silver.filter(pl.col("application_number").is_not_null() & (pl.col("patent_number").is_not_null()))


def transform_exclusivity(file_path: Path) -> pl.DataFrame:
    """
    Transform raw exclusivity files into the Silver schema.
    """
    logger.info(f"Transforming exclusivity file: {file_path}")
    df = _clean_read_csv(file_path)

    if df.is_empty():
        return df

    # Global string cleaning for values
    df = df.with_columns(pl.all().cast(pl.String).str.strip_chars())

    col_map = {c.lower(): c for c in df.columns}

    def safe_col(name: str) -> pl.Expr:
        real_name = col_map.get(name.lower())
        if real_name:
            return pl.col(real_name)
        return pl.lit(None).cast(pl.String)

    def safe_col_str(name: str) -> pl.Expr:
        real_name = col_map.get(name.lower())
        if real_name:
            return pl.col(real_name).cast(pl.String)
        return pl.lit(None).cast(pl.String)

    df_silver = df.select(
        [
            safe_col_str("Appl_No").str.pad_start(6, "0").alias("application_number"),
            safe_col_str("Product_No").str.pad_start(3, "0").alias("product_number"),
            safe_col("Exclusivity_Code").alias("exclusivity_code"),
            safe_col("Exclusivity_Date")
            .map_elements(lambda x: _parse_fda_date(x), return_dtype=pl.String)
            .str.to_date("%Y-%m-%d", strict=False)
            .alias("exclusivity_end_date"),
        ]
    )

    return df_silver.filter(pl.col("application_number").is_not_null() & (pl.col("exclusivity_code").is_not_null()))
