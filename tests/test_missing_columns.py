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

    def test_products_missing_columns(self, tmp_path: Path):
        """Test products file missing required columns."""
        # Create file with only one column
        f_path = tmp_path / "partial.txt"
        # Since missing columns will be Null, source_id logic might yield Null/empty
        # and filter at end of transform removes them.
        # "source_id" = appl_no + product_no + status
        # If appl_no or product_no is null, result is null (polars default propagation).
        # So the result DF will be empty due to filter.

        # To hit the branch "if real_name:" being False (and returning pl.lit(None)),
        # we need to ensure the script doesn't crash but returns the computed column as None.
        # Since we filter source_id is not null at end, an empty DF is expected if ID components are missing.
        # This confirms "missing column -> None" logic worked without crashing.

        f_path.write_text("Ingredient\nIngA", encoding="utf-8")

        df = transform_products(f_path)
        assert df.is_empty()

    def test_patents_missing_columns(self, tmp_path: Path):
        """Test patents file missing required columns."""
        f_path = tmp_path / "partial_pat.txt"
        # Just enough to pass filter? Appl_No and Patent_No required.
        # But we want to test missing *other* columns that use safe_col_str
        f_path.write_text("Appl_No~Product_No~Patent_No\n1~1~1", encoding="utf-8")

        df = transform_patents(f_path)
        row = df.row(0, named=True)
        # Patent_Expire_Date_Text missing -> None
        assert row["patent_expiry_date"] is None
        # We also need to test a column handled by safe_col_str missing.
        # Patents uses safe_col_str for Appl_No, Product_No, Patent_No (all present here).
        # Need to remove one to hit branch, but then it gets filtered.
        # The coverage tool tracks the execution of `return pl.lit(None).cast(pl.String)` inside safe_col_str.
        # We need a call to safe_col_str for a missing column that *doesn't* result in filtering
        # OR just execute it.
        # In transform_patents, safe_col_str is used for: Appl_No, Product_No, Patent_No.
        # All are required by filter.
        # So to hit `else` branch of safe_col_str, we must miss one of these, but then the row is filtered.
        # Even if filtered, the select expression is evaluated/built.
        # Wait, coverage checks if the line was executed. Building the expression executes the python function `safe_col_str`.
        # So even if result DF is empty, the python code ran.
        pass

    def test_patents_missing_key_column(self, tmp_path: Path):
        """Test missing key column (Appl_No) to hit safe_col_str fallback."""
        f_path = tmp_path / "missing_key.txt"
        f_path.write_text("Product_No~Patent_No\n1~1", encoding="utf-8")
        df = transform_patents(f_path)
        assert df.is_empty()

    def test_exclusivity_missing_key_column(self, tmp_path: Path):
        """Test missing key column to hit safe_col_str fallback in exclusivity."""
        f_path = tmp_path / "missing_key_exc.txt"
        f_path.write_text("Product_No~Exclusivity_Code\n1~E", encoding="utf-8")
        df = transform_exclusivity(f_path)
        assert df.is_empty()

    def test_exclusivity_missing_columns(self, tmp_path: Path):
        """Test exclusivity file missing required columns."""
        f_path = tmp_path / "partial_exc.txt"
        f_path.write_text("Appl_No~Product_No~Exclusivity_Code\n1~1~E", encoding="utf-8")

        df = transform_exclusivity(f_path)
        row = df.row(0, named=True)
        # Exclusivity_Date missing -> None
        assert row["exclusivity_end_date"] is None
