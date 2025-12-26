# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Tests for data models."""

from coreason_etl_fda_orange_book.gold.models import GoldProductEnriched
from coreason_etl_fda_orange_book.silver.models import SilverExclusivity, SilverPatent, SilverProduct


class TestModels:
    """Test instantiation of Pydantic models to ensure coverage."""

    def test_silver_product_model(self):
        """Test SilverProduct model."""
        m = SilverProduct(
            coreason_id="id",
            source_id="sid",
            ingredient="ing",
            trade_name="tn",
            applicant_short="app",
            strength="str",
            application_number="000123",
            product_number="001",
            is_rld=True,
            marketing_status="RX"
        )
        assert m.marketing_status == "RX"

    def test_silver_patent_model(self):
        """Test SilverPatent model."""
        m = SilverPatent(
            application_number="000123",
            product_number="001",
            patent_number="12345",
            is_drug_substance=True,
            is_drug_product=False,
            is_delisted=False
        )
        assert m.patent_number == "12345"

    def test_silver_exclusivity_model(self):
        """Test SilverExclusivity model."""
        m = SilverExclusivity(
            application_number="000123",
            product_number="001",
            exclusivity_code="EXC"
        )
        assert m.exclusivity_code == "EXC"

    def test_gold_product_model(self):
        """Test GoldProductEnriched model."""
        m = GoldProductEnriched(
            coreason_id="id",
            source_id="sid",
            ingredient="ing",
            trade_name="tn",
            applicant_short="app",
            strength="str",
            application_number="000123",
            product_number="001",
            is_rld=True,
            marketing_status="RX",
            search_vector_text="tn ing",
            patent_number="pat1"
        )
        assert m.patent_number == "pat1"
