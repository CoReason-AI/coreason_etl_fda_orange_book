# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Pydantic models for the Silver layer."""

from datetime import date
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class SilverProduct(BaseModel):
    """
    Silver layer model for FDA Orange Book Products.
    """

    model_config = ConfigDict(frozen=True)

    coreason_id: str = Field(description="UUID5 generated from Namespace FDA + Source ID")
    source_id: str = Field(description="Composite key: Appl_No + Product_No + Type")
    ingredient: str
    trade_name: str
    applicant_short: str
    strength: str
    application_number: str = Field(description="6-digit padded application number")
    product_number: str = Field(description="3-digit padded product number")
    te_code: Optional[str] = None
    approval_date: Optional[date] = None
    is_rld: bool
    marketing_status: str = Field(description="RX, OTC, or DISCN")


class SilverPatent(BaseModel):
    """
    Silver layer model for FDA Orange Book Patents.
    """

    model_config = ConfigDict(frozen=True)

    application_number: str = Field(description="6-digit padded application number")
    product_number: str = Field(description="3-digit padded product number")
    patent_number: str
    patent_expiry_date: Optional[date] = None
    is_drug_substance: bool
    is_drug_product: bool
    patent_use_code: Optional[str] = None
    is_delisted: bool
    submission_date: Optional[date] = None


class SilverExclusivity(BaseModel):
    """
    Silver layer model for FDA Orange Book Exclusivity.
    """

    model_config = ConfigDict(frozen=True)

    application_number: str = Field(description="6-digit padded application number")
    product_number: str = Field(description="3-digit padded product number")
    exclusivity_code: str
    exclusivity_end_date: Optional[date] = None
