# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Tests targeting helper functions in transform module."""

from pathlib import Path
from unittest.mock import patch

from coreason_etl_fda_orange_book.silver.transform import _clean_read_csv, _parse_fda_date


class TestTransformHelpers:
    """Tests for helper functions in transform.py."""

    def test_clean_read_csv_exception(self, tmp_path: Path):
        """Test safe reading of CSV when exception occurs."""
        # Using a directory path instead of file will cause read_csv to fail (IsADirectoryError)
        # or file not found if we give non-existent
        # read_csv raises ComputeError or similar.

        # Testing with non-existent file
        f_path = tmp_path / "non_existent.txt"

        # We want to ensure it catches exception and returns empty DF
        # But wait, Polars read_csv might raise different errors.
        # Let's mock pl.read_csv to raise Exception to be sure we hit the except block

        with patch("polars.read_csv", side_effect=Exception("Boom")):
            df = _clean_read_csv(f_path)
            assert df.is_empty()

    def test_parse_fda_date_value_error(self):
        """Test ValueError handling in date parser."""
        # "Invalid" date string that doesn't match format
        assert _parse_fda_date("Not a date") is None
