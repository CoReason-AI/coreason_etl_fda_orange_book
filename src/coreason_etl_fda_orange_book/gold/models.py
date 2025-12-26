# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Pydantic models for the Gold layer."""

from datetime import date
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from coreason_etl_fda_orange_book.silver.models import SilverExclusivity, SilverPatent, SilverProduct


class GoldProductEnriched(SilverProduct):
    """
    Gold layer model: Denormalized Product view enriched with Patents and Exclusivity.
    """

    model_config = ConfigDict(frozen=True)

    # Enriched Fields
    search_vector_text: str = Field(description="Concatenation of trade_name and ingredient")

    # Nested lists or flattened joins?
    # FRD says "Join: Products LEFT JOIN Patents ... LEFT JOIN Exclusivity"
    # Usually this implies 1:N expansion or aggregation.
    # If we flatten, we get many rows per product.
    # Let's assume a flattened denormalized view or nested if DLT supports JSON.
    # DLT with Postgres usually flattens lists into child tables or JSONB.
    # Given the goal "Denormalized 'High Value' view", distinct rows for product+patent combinations might be expected,
    # OR a Product row with lists of patents.
    # The FRD doesn't specify nested vs flat, but "LEFT JOIN" usually implies SQL flat results.
    # However, to avoid explosion, let's keep it as logical object with optional nested data?
    # Actually, standard SQL joins explode rows.
    # Let's support the exploded view for simplicity of SQL consumption.

    # Actually, let's include the joined columns as Optional (since LEFT JOIN).
    # If a product has multiple patents, this row repeats with different patent info.

    # Patent Info (Nullable)
    patent_number: Optional[str] = None
    patent_expiry_date: Optional[date] = None
    is_drug_substance: Optional[bool] = None
    is_drug_product: Optional[bool] = None
    patent_use_code: Optional[str] = None
    is_delisted: Optional[bool] = None

    # Exclusivity Info (Nullable)
    exclusivity_code: Optional[str] = None
    exclusivity_end_date: Optional[date] = None
