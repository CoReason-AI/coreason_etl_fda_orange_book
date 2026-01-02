# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""
Edge case tests for Silver layer transformations.
Focuses on system limits (memory), network/IO failures, and data corruption.
"""

from pathlib import Path
from unittest.mock import patch

import polars as pl

from coreason_etl_fda_orange_book.silver.transform import (
    transform_exclusivity,
    transform_patents,
    transform_products,
)


import pytest

def test_transform_memory_error(tmp_path: Path) -> None:
    """
    Test that transform_products gracefully handles a MemoryError during file reading.
    Simulates hitting memory limits when loading a large CSV.
    """
    # Create a dummy file so the path exists (though mock prevents reading it)
    dummy_file = tmp_path / "dummy.txt"
    dummy_file.touch()

    # Mock polars.read_csv to raise MemoryError
    with patch("polars.read_csv", side_effect=MemoryError("Out of memory")):
        with pytest.raises(MemoryError, match="Out of memory"):
            transform_products(dummy_file)


def test_transform_network_failure(tmp_path: Path) -> None:
    """
    Test that transform_patents gracefully handles an OSError (e.g., network failure).
    Simulates a network drive becoming unavailable during read.
    """
    dummy_file = tmp_path / "network_file.txt"
    dummy_file.touch()

    # Mock polars.read_csv to raise OSError
    with patch("polars.read_csv", side_effect=OSError("Network unreachable")):
        with pytest.raises(OSError, match="Network unreachable"):
            transform_patents(dummy_file)


def test_large_ragged_file(tmp_path: Path) -> None:
    """
    Test handling of a file with ragged lines (inconsistent column counts).
    This validates that the 'truncate_ragged_lines=True' option in _clean_read_csv works
    and prevents crashes on malformed data.
    """
    ragged_file = tmp_path / "ragged.txt"

    # Create content with a header and rows having different numbers of columns
    # Header has 4 columns.
    # Row 1 has 4 columns (Good)
    # Row 2 has 6 columns (Too many - should be truncated or handled)
    # Row 3 has 2 columns (Too few - might be filled with nulls or cause issues if not handled)

    # Note: transform_exclusivity expects specific columns:
    # Appl_No, Product_No, Exclusivity_Code, Exclusivity_Date

    content = (
        "Appl_No~Product_No~Exclusivity_Code~Exclusivity_Date\n"
        "000001~001~E1~Jan 1, 2025\n"
        "000002~001~E2~Jan 1, 2025~EXTRA~DATA\n"  # Ragged line (too long)
        "000003~001\n"  # Ragged line (too short)
    )

    ragged_file.write_text(content, encoding="utf-8")

    # We expect _clean_read_csv to handle this.
    # verify it returns a dataframe with what it could parse.
    df = transform_exclusivity(ragged_file)

    assert isinstance(df, pl.DataFrame)

    # Check if we got at least the valid row.
    # '000001' should definitely be there.
    valid_rows = df.filter(pl.col("application_number") == "000001")
    assert not valid_rows.is_empty()

    # Ensure it didn't crash and returned something.
    assert df.height >= 1
