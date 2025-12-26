# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Tests for Bronze layer ingestion logic."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from coreason_etl_fda_orange_book.bronze.ingestion import bronze_resource, yield_bronze_records
from coreason_etl_fda_orange_book.source import FdaOrangeBookSource


@pytest.fixture
def mock_source() -> MagicMock:
    """Mock FdaOrangeBookSource."""
    source = MagicMock(spec=FdaOrangeBookSource)
    source.calculate_file_hash.return_value = "mock_hash"
    return source


def test_yield_bronze_records_basic(mock_source: MagicMock, tmp_path: Path) -> None:
    """Test basic record yielding."""
    f1 = tmp_path / "products.txt"
    f1.write_text("line1\nline2", encoding="utf-8")

    files_map = {"products": [f1]}

    records = list(yield_bronze_records(files_map, mock_source))

    assert len(records) == 2
    assert records[0]["source_file"] == "products.txt"
    assert records[0]["source_hash"] == "mock_hash"
    assert records[0]["raw_content"]["data"] == "line1"
    assert records[0]["role"] == "products"

    assert records[1]["raw_content"]["data"] == "line2"


def test_yield_bronze_records_multiple_files(mock_source: MagicMock, tmp_path: Path) -> None:
    """Test yielding from multiple files/roles."""
    f1 = tmp_path / "rx.txt"
    f1.write_text("rx_data", encoding="utf-8")
    f2 = tmp_path / "patent.txt"
    f2.write_text("patent_data", encoding="utf-8")

    files_map = {"products": [f1], "patent": [f2]}

    records = list(yield_bronze_records(files_map, mock_source))

    # Sort by role/file to ensure order for assertion
    records.sort(key=lambda x: x["source_file"])

    assert len(records) == 2
    assert records[0]["role"] == "patent"
    assert records[0]["raw_content"]["data"] == "patent_data"
    assert records[1]["role"] == "products"
    assert records[1]["raw_content"]["data"] == "rx_data"


def test_yield_bronze_records_empty_lines(mock_source: MagicMock, tmp_path: Path) -> None:
    """Test skipping empty lines."""
    f1 = tmp_path / "data.txt"
    f1.write_text("line1\n\nline3", encoding="utf-8")

    files_map = {"test": [f1]}
    records = list(yield_bronze_records(files_map, mock_source))

    assert len(records) == 2
    assert records[0]["raw_content"]["data"] == "line1"
    assert records[1]["raw_content"]["data"] == "line3"


def test_yield_bronze_records_hash_failure(mock_source: MagicMock, tmp_path: Path) -> None:
    """Test behavior when hashing fails."""
    f1 = tmp_path / "bad.txt"
    f1.touch()

    mock_source.calculate_file_hash.side_effect = Exception("Hash error")

    files_map = {"test": [f1]}
    records = list(yield_bronze_records(files_map, mock_source))

    assert len(records) == 0


def test_yield_bronze_records_read_failure(mock_source: MagicMock, tmp_path: Path) -> None:
    """Test behavior when reading fails."""
    f1 = tmp_path / "unreadable.txt"
    f1.touch()

    files_map = {"test": [f1]}

    with patch("builtins.open", side_effect=OSError("Read error")):
        records = list(yield_bronze_records(files_map, mock_source))

    assert len(records) == 0


def test_yield_bronze_records_large_file(mock_source: MagicMock, tmp_path: Path) -> None:
    """Test handling of a large file (simulated)."""
    f1 = tmp_path / "large.txt"
    # Create 1000 lines
    content = "\n".join([f"line_{i}" for i in range(1000)])
    f1.write_text(content, encoding="utf-8")

    files_map = {"test": [f1]}
    records = list(yield_bronze_records(files_map, mock_source))

    assert len(records) == 1000
    assert records[0]["raw_content"]["data"] == "line_0"
    assert records[-1]["raw_content"]["data"] == "line_999"


def test_yield_bronze_records_encoding_error(mock_source: MagicMock, tmp_path: Path) -> None:
    """
    Test handling of encoding errors.
    We write bytes that are invalid in UTF-8.
    Since configuration is ENCODING_ERRORS='replace', it should not crash.
    """
    f1 = tmp_path / "binary.txt"
    # 0x80 is invalid in UTF-8
    f1.write_bytes(b"invalid\x80byte")

    files_map = {"test": [f1]}
    records = list(yield_bronze_records(files_map, mock_source))

    assert len(records) == 1
    # Expect replacement character U+FFFD (often printed as ) or just passing through if permissive?
    # Python 'replace' error handler inserts U+FFFD.
    assert "invalid" in records[0]["raw_content"]["data"]
    # Check that it didn't crash and yielded something
    assert len(records[0]["raw_content"]["data"]) > 0


def test_bronze_resource_wrapper(mock_source: MagicMock, tmp_path: Path) -> None:
    """Test the dlt resource wrapper calls the generator."""
    f1 = tmp_path / "test.txt"
    f1.write_text("content", encoding="utf-8")

    files_map = {"test": [f1]}

    # Check it is a resource
    assert callable(bronze_resource)

    # Iterate
    resource = bronze_resource(files_map, mock_source)
    records = list(resource)

    assert len(records) == 1
    assert records[0]["raw_content"]["data"] == "content"


def test_yield_bronze_records_race_condition_file_deleted(mock_source: MagicMock, tmp_path: Path) -> None:
    """
    Test a simulated race condition (TOCTOU).
    The file exists during hashing, but is deleted before it can be opened.
    The generator should catch the read error and skip it without crashing.
    """
    f1 = tmp_path / "race.txt"
    f1.touch()

    def delete_file_side_effect(*args):
        # Simulate file deletion right after hashing
        if f1.exists():
            f1.unlink()
        return "mock_hash"

    mock_source.calculate_file_hash.side_effect = delete_file_side_effect

    files_map = {"test": [f1]}
    # This should log an error but yield 0 records and NOT raise exception
    records = list(yield_bronze_records(files_map, mock_source))

    assert len(records) == 0


def test_yield_bronze_records_empty_file_list(mock_source: MagicMock) -> None:
    """Test handling of a role with an empty list of files."""
    files_map = {"empty_role": []}

    records = list(yield_bronze_records(files_map, mock_source))
    assert len(records) == 0


def test_yield_bronze_records_very_long_line(mock_source: MagicMock, tmp_path: Path) -> None:
    """Test handling of an extremely long line (e.g. 100k chars)."""
    f1 = tmp_path / "long_line.txt"
    long_data = "a" * 100000
    f1.write_text(long_data, encoding="utf-8")

    files_map = {"test": [f1]}
    records = list(yield_bronze_records(files_map, mock_source))

    assert len(records) == 1
    assert records[0]["raw_content"]["data"] == long_data


def test_yield_bronze_records_bom_handling(mock_source: MagicMock, tmp_path: Path) -> None:
    """
    Test handling of Byte Order Mark (BOM).
    Standard 'utf-8' encoding in Python treats BOM (\ufeff) as a character (ZWNBSP).
    'utf-8-sig' would strip it.
    This test verifies that the configured behavior (utf-8) is respected, meaning BOM is preserved as content.
    """
    f1 = tmp_path / "bom.txt"
    content = "data"
    # Write with BOM
    f1.write_text(f"\ufeff{content}", encoding="utf-8")

    files_map = {"test": [f1]}
    records = list(yield_bronze_records(files_map, mock_source))

    assert len(records) == 1
    # Expect \ufeff to be part of the data string
    assert records[0]["raw_content"]["data"] == "\ufeffdata"
