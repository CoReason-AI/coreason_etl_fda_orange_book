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
import dlt

from coreason_etl_fda_orange_book.bronze.ingestion import yield_bronze_records, bronze_resource
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
    assert hasattr(bronze_resource, "__call__")

    # Iterate
    resource = bronze_resource(files_map, mock_source)
    records = list(resource)

    assert len(records) == 1
    assert records[0]["raw_content"]["data"] == "content"
