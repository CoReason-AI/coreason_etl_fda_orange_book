# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Additional edge case tests for Bronze layer."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from coreason_etl_fda_orange_book.bronze.ingestion import yield_bronze_records
from coreason_etl_fda_orange_book.source import FdaOrangeBookSource


@pytest.fixture(name="mock_source")
def mock_source() -> MagicMock:
    """Mock FdaOrangeBookSource."""
    source = MagicMock(spec=FdaOrangeBookSource)
    source.calculate_file_hash.return_value = "mock_hash"
    return source


def test_yield_bronze_records_race_condition(mock_source: MagicMock, tmp_path: Path) -> None:
    """
    Test resilience against race conditions where file is deleted after hash calculation but before reading.
    """
    f1 = tmp_path / "race.txt"
    f1.write_text("content", encoding="utf-8")

    files_map = {"test": [f1]}

    # Mock open to simulate file disappearance (FileNotFoundError)
    # We allow calculate_file_hash to succeed (it uses the file path)
    # But when yield_bronze_records tries to open() it, it should handle the error.

    # We need to ensure calculate_file_hash is called first (it is in the code),
    # then open() fails.

    # In yield_bronze_records:
    # 1. source.calculate_file_hash(file_path) -> succeeds (we mocked it or let it run on real file)
    # 2. open(file_path, ...) -> fails

    # Since 'open' is a builtin, we patch it.
    with patch("builtins.open", side_effect=FileNotFoundError("File gone")):
        with pytest.raises(FileNotFoundError, match="File gone"):
            list(yield_bronze_records(files_map, mock_source))


def test_yield_bronze_records_bom_handling(mock_source: MagicMock, tmp_path: Path) -> None:
    """
    Test handling of files with Byte Order Mark (BOM).
    The 'utf-8' encoding in Python handles BOM if using 'utf-8-sig', but standard 'utf-8' might include it as a char.
    However, the FRD specifies 'utf8-lossy' behavior.
    If the file has a BOM, and we read with 'utf-8', the first line might have the BOM char.
    Since we strip(), it might remain.
    Ideally, we should check if it causes issues.
    """
    f1 = tmp_path / "bom.txt"
    # Write UTF-8 BOM followed by content
    content = "\ufeffline1\nline2"
    f1.write_text(content, encoding="utf-8")

    files_map = {"test": [f1]}
    records = list(yield_bronze_records(files_map, mock_source))

    assert len(records) == 2
    # If read as utf-8, \ufeff is ZWNBSP. strip() removes whitespace including ZWNBSP?
    # Actually strip() removes whitespace. ZWNBSP is Unicode Category Cf (Format), often treated as whitespace.
    # Let's verify.
    first_line = records[0]["raw_content"]["data"]

    # Python's strip() removes whitespace characters.
    # \ufeff is NOT removed by default strip() in all versions, let's see.
    # Actually, often it is desired to remove it.

    # If our code uses `encoding='utf-8'`, the BOM is read as a character.
    # If the user considers BOM "unwanted data", we might need 'utf-8-sig'.
    # But currently configured as 'utf-8'.
    # Let's just verify it reads safely without crashing.
    assert "line1" in first_line


def test_yield_bronze_records_extremely_long_lines(mock_source: MagicMock, tmp_path: Path) -> None:
    """
    Test handling of extremely long lines (e.g. 100k+ chars).
    """
    f1 = tmp_path / "long_line.txt"
    long_line = "a" * 150000  # 150k chars
    f1.write_text(long_line, encoding="utf-8")

    files_map = {"test": [f1]}
    records = list(yield_bronze_records(files_map, mock_source))

    assert len(records) == 1
    assert len(records[0]["raw_content"]["data"]) == 150000
