# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""System resilience and fault tolerance tests."""

import errno
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import polars as pl
from loguru import logger

from coreason_etl_fda_orange_book.bronze.ingestion import yield_bronze_records
from coreason_etl_fda_orange_book.source import FdaOrangeBookSource
from coreason_etl_fda_orange_book.silver.transform import transform_products, transform_patents

# Mock source
@pytest.fixture
def mock_source():
    source = MagicMock(spec=FdaOrangeBookSource)
    source.calculate_file_hash.return_value = "mock_hash"
    return source

# 1. Permission Denied Tests

def test_read_permission_error(mock_source, tmp_path):
    """Test resilience when reading a file fails due to permissions."""
    f1 = tmp_path / "protected.txt"
    f1.write_text("data", encoding="utf-8")

    files_map = {"test": [f1]}

    # Mock open to raise PermissionError
    # We patch builtins.open
    with patch("builtins.open", side_effect=PermissionError("Permission denied")):
        records = list(yield_bronze_records(files_map, mock_source))

    # Should handle error gracefully and yield nothing (log error instead of crash)
    assert len(records) == 0

def test_write_permission_error(tmp_path):
    """Test resilience when writing a downloaded file fails due to permissions."""
    source = FdaOrangeBookSource()
    dest = tmp_path / "readonly_dir" / "file.zip"

    # We don't actually create a readonly dir to avoid OS dependent issues in test env,
    # instead we mock the open() call used during download/write.

    # Mock requests.get
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.iter_content = lambda chunk_size: [b"chunk"]
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__.return_value = None

    with patch("requests.get", return_value=mock_response):
        # Patch open to fail when writing
        with patch("builtins.open", side_effect=PermissionError("Write permission denied")):
            with pytest.raises(PermissionError):
                # The source code doesn't explicitly catch PermissionError during download,
                # so it should propagate. We verify it is raised correctly.
                # If the requirement was to suppress it, we'd assert it didn't raise.
                # Typically, failing to write the core source file IS a critical error, so raising is correct.
                source.download_archive(dest)


# 2. File System Lock Tests

def test_file_locked(mock_source, tmp_path):
    """Test resilience when a file is locked by another process (BlockingIOError)."""
    f1 = tmp_path / "locked.txt"
    f1.write_text("data", encoding="utf-8")
    files_map = {"test": [f1]}

    with patch("builtins.open", side_effect=BlockingIOError("Resource temporarily unavailable")):
        # Depending on implementation, might retry or skip.
        # Current implementation of yield_bronze_records catches Exception.
        records = list(yield_bronze_records(files_map, mock_source))

    assert len(records) == 0

# 3. Resource Exhaustion Tests

def test_disk_full(tmp_path):
    """Test behavior when disk is full (ENOSPC) during download."""
    source = FdaOrangeBookSource()
    dest = tmp_path / "full_disk.zip"

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.iter_content = lambda chunk_size: [b"chunk"]
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__.return_value = None

    # Simulate ENOSPC on write
    def raise_enospc(*args, **kwargs):
        raise OSError(errno.ENOSPC, "No space left on device")

    mock_file = MagicMock()
    mock_file.write.side_effect = raise_enospc
    mock_file.__enter__.return_value = mock_file
    mock_file.__exit__.return_value = None

    with patch("requests.get", return_value=mock_response):
        with patch("builtins.open", return_value=mock_file):
            with pytest.raises(OSError) as excinfo:
                source.download_archive(dest)
            assert excinfo.value.errno == errno.ENOSPC

def test_memory_exhaustion_transform(tmp_path):
    """
    Test resilience when transformation runs out of memory.
    We mock polars.read_csv to raise MemoryError.
    """
    f1 = tmp_path / "large.txt"
    f1.touch()

    # If memory error occurs, the transform function handles it?
    # Looking at _clean_read_csv in transform.py, it catches Exception and logs error, returning empty DF.
    # So it should NOT crash, but return empty DataFrame.

    with patch("polars.read_csv", side_effect=MemoryError("Out of memory")):
        df = transform_products(f1)

    # Verify it returned an empty DataFrame instead of crashing
    assert isinstance(df, pl.DataFrame)
    assert df.is_empty()

def test_corrupted_transformation_input(tmp_path):
    """Test transformation with a file that is not a valid CSV (logical corruption)."""
    f1 = tmp_path / "corrupt.txt"
    # Write binary garbage
    f1.write_bytes(b"\x80\x81\xFF garbage")

    # polars.read_csv might fail or read garbage.
    # If we use encoding='utf8-lossy', it might survive but produce garbage.
    # If structure is totally wrong, it might fail schema inference.

    # Real polars behavior:
    # If separators are missing, it might read as 1 column.

    # We want to ensure it doesn't crash Python.
    df = transform_products(f1)
    assert isinstance(df, pl.DataFrame)
    # It might have 0 rows or some rows depending on how robust read_csv is.
    # The key is successful return.
