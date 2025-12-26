# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Tests targeting missing column branches in transform logic."""

from pathlib import Path

from coreason_etl_fda_orange_book.silver.transform import (
    transform_exclusivity,
    transform_patents,
    transform_products,
)


class TestMissingColumns:
    """Tests for missing columns (fallback to None)."""

    def test_products_missing_columns(self, tmp_path: Path) -> None:
        """Test products file missing required columns."""
        # Create file with only one column
        f_path = tmp_path / "partial.txt"
        # Since missing columns will be Null, source_id logic might yield Null/empty
        # and filter at end of transform removes them.

        f_path.write_text("Ingredient\nIngA", encoding="utf-8")

        df = transform_products(f_path)
        assert df.is_empty()

    def test_patents_missing_columns(self, tmp_path: Path) -> None:
        """Test patents file missing required columns."""
        f_path = tmp_path / "partial_pat.txt"
        # Just enough to pass filter? Appl_No and Patent_No required.
        # But we want to test missing *other* columns that use safe_col_str
        f_path.write_text("Appl_No~Product_No~Patent_No\n1~1~1", encoding="utf-8")

        df = transform_patents(f_path)
        row = df.row(0, named=True)
        # Patent_Expire_Date_Text missing -> None
        assert row["patent_expiry_date"] is None

    def test_patents_missing_key_column(self, tmp_path: Path) -> None:
        """Test missing key column (Appl_No) to hit safe_col_str fallback."""
        f_path = tmp_path / "missing_key.txt"
        f_path.write_text("Product_No~Patent_No\n1~1", encoding="utf-8")
        df = transform_patents(f_path)
        assert df.is_empty()

    def test_exclusivity_missing_key_column(self, tmp_path: Path) -> None:
        """Test missing key column to hit safe_col_str fallback in exclusivity."""
        f_path = tmp_path / "missing_key_exc.txt"
        f_path.write_text("Product_No~Exclusivity_Code\n1~E", encoding="utf-8")
        df = transform_exclusivity(f_path)
        assert df.is_empty()

    def test_exclusivity_missing_columns(self, tmp_path: Path) -> None:
        """Test exclusivity file missing required columns."""
        f_path = tmp_path / "partial_exc.txt"
        f_path.write_text("Appl_No~Product_No~Exclusivity_Code\n1~1~E", encoding="utf-8")

        df = transform_exclusivity(f_path)
        row = df.row(0, named=True)
        # Exclusivity_Date missing -> None
        assert row["exclusivity_end_date"] is None
