# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Tests for the hashing functionality in FdaOrangeBookSource."""

import hashlib
from pathlib import Path
from unittest.mock import patch

import pytest

from coreason_etl_fda_orange_book.exceptions import SourceConnectionError
from coreason_etl_fda_orange_book.source import FdaOrangeBookSource


@pytest.fixture(name="fda_source")
def fda_source() -> FdaOrangeBookSource:
    """Fixture for FdaOrangeBookSource."""
    return FdaOrangeBookSource()


def test_calculate_file_hash_success(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test successful hash calculation."""
    test_file = tmp_path / "data.txt"
    content = b"hello world"
    test_file.write_bytes(content)

    expected_hash = hashlib.md5(content).hexdigest()
    assert fda_source.calculate_file_hash(test_file) == expected_hash


def test_calculate_file_hash_empty(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test hash calculation for empty file."""
    test_file = tmp_path / "empty.txt"
    test_file.write_bytes(b"")

    expected_hash = hashlib.md5(b"").hexdigest()
    assert fda_source.calculate_file_hash(test_file) == expected_hash


def test_calculate_file_hash_large(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test hash calculation for file larger than chunk size."""
    test_file = tmp_path / "large.txt"
    # Create content larger than default chunk size (8192)
    content = b"a" * 10000
    test_file.write_bytes(content)

    expected_hash = hashlib.md5(content).hexdigest()
    assert fda_source.calculate_file_hash(test_file) == expected_hash


def test_calculate_file_hash_missing_file(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test hash calculation raises error if file missing."""
    test_file = tmp_path / "missing.txt"
    with pytest.raises(SourceConnectionError, match="File not found"):
        fda_source.calculate_file_hash(test_file)


def test_calculate_file_hash_read_error(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test hash calculation handles read errors."""
    test_file = tmp_path / "protected.txt"
    test_file.touch()

    # Simulate an OSError during open or read
    with patch("builtins.open", side_effect=OSError("Permission denied")):
        with pytest.raises(SourceConnectionError, match="Failed to calculate hash"):
            fda_source.calculate_file_hash(test_file)
