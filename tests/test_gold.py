# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Tests for Gold layer logic."""

from datetime import date

import polars as pl
import pytest

from coreason_etl_fda_orange_book.gold.logic import create_gold_view


class TestGoldLogic:
    """Tests for Gold layer transformations and joins."""

    @pytest.fixture(name="silver_products")
    def silver_products(self) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "application_number": ["000111", "000222", "000333"],
                "product_number": ["001", "001", "001"],
                "trade_name": ["DrugA", "DrugB", "DrugC"],
                "ingredient": ["IngA", "IngB", "IngC"],
                "marketing_status": ["RX", "RX", "DISCN"],
                "coreason_id": ["id1", "id2", "id3"],
                # Add other required cols to satisfy schema if checked, or minimal for join
                "applicant_short": ["AppA", "AppB", "AppC"],
                "strength": ["10mg", "20mg", "30mg"],
                "is_rld": [True, False, False],
                "source_id": ["s1", "s2", "s3"],
            }
        )

    @pytest.fixture(name="silver_patents")
    def silver_patents(self) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "application_number": ["000111"],
                "product_number": ["001"],
                "patent_number": ["PAT123"],
                "patent_expiry_date": [date(2030, 1, 1)],
                "is_drug_substance": [True],
                "is_drug_product": [False],
                "patent_use_code": ["U-1"],
                "is_delisted": [False],
                "submission_date": [date(2010, 1, 1)],
            }
        )

    @pytest.fixture(name="silver_exclusivity")
    def silver_exclusivity(self) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "application_number": ["000222"],
                "product_number": ["001"],
                "exclusivity_code": ["EX1"],
                "exclusivity_end_date": [date(2028, 1, 1)],
            }
        )

    def test_create_gold_view_basic(
        self, silver_products: pl.DataFrame, silver_patents: pl.DataFrame, silver_exclusivity: pl.DataFrame
    ) -> None:
        """Test basic join and enrichment."""
        df = create_gold_view(silver_products, silver_patents, silver_exclusivity)

        # Should filter DISCN by default
        assert df.height == 2

        # Check Enrichment
        row1 = df.filter(pl.col("application_number") == "000111").row(0, named=True)
        assert row1["search_vector_text"] == "DrugA IngA"
        assert row1["patent_number"] == "PAT123"  # Joined
        assert row1["exclusivity_code"] is None  # No exclusivity for this one

        row2 = df.filter(pl.col("application_number") == "000222").row(0, named=True)
        assert row2["patent_number"] is None  # No patent
        assert row2["exclusivity_code"] == "EX1"  # Joined

    def test_create_gold_view_include_discontinued(
        self, silver_products: pl.DataFrame, silver_patents: pl.DataFrame, silver_exclusivity: pl.DataFrame
    ) -> None:
        """Test including discontinued products."""
        df = create_gold_view(silver_products, silver_patents, silver_exclusivity, include_discontinued=True)
        assert df.height == 3
        assert "000333" in df["application_number"].to_list()

    def test_empty_inputs(self) -> None:
        """Test handling of empty inputs."""
        empty_df = pl.DataFrame()
        assert create_gold_view(empty_df, empty_df, empty_df).is_empty()

    def test_missing_patents_exclusivity(self, silver_products: pl.DataFrame) -> None:
        """Test when patents or exclusivity are empty."""
        df = create_gold_view(silver_products, pl.DataFrame(), pl.DataFrame())
        assert df.height == 2  # RX only
        assert "patent_number" in df.columns
        assert "exclusivity_code" in df.columns
        assert df["patent_number"].null_count() == 2
