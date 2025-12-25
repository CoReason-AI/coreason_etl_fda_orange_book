# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Tests for the FdaOrangeBookSource class."""

import hashlib
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from coreason_etl_fda_orange_book.config import FdaConfig
from coreason_etl_fda_orange_book.exceptions import SourceConnectionError, SourceSchemaError
from coreason_etl_fda_orange_book.source import FdaOrangeBookSource


@pytest.fixture
def fda_source() -> FdaOrangeBookSource:
    """Fixture for FdaOrangeBookSource."""
    return FdaOrangeBookSource("http://mock-fda.gov/download")


def test_init_default_url() -> None:
    """Test that the default URL is used when none is provided."""
    source = FdaOrangeBookSource()
    assert source.base_url == FdaConfig.DEFAULT_BASE_URL


def test_download_archive_success(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test successful download."""
    target_file = tmp_path / "test.zip"

    # Mock requests.get
    mock_response = MagicMock()
    mock_response.iter_content.return_value = [b"chunk1", b"chunk2"]
    mock_response.raise_for_status.return_value = None

    with patch("requests.get", return_value=mock_response) as mock_get:
        mock_get.return_value.__enter__.return_value = mock_response
        fda_source.download_archive(target_file)

    assert target_file.exists()
    assert target_file.read_bytes() == b"chunk1chunk2"


def test_download_archive_failure(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test download failure raises SourceConnectionError."""
    target_file = tmp_path / "test.zip"

    with patch("requests.get", side_effect=requests.RequestException("Boom")):
        with pytest.raises(SourceConnectionError, match="Failed to download"):
            fda_source.download_archive(target_file)


def test_download_empty_content(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test download of empty content."""
    target_file = tmp_path / "empty.zip"

    mock_response = MagicMock()
    mock_response.iter_content.return_value = []
    mock_response.raise_for_status.return_value = None

    with patch("requests.get", return_value=mock_response) as mock_get:
        mock_get.return_value.__enter__.return_value = mock_response
        fda_source.download_archive(target_file)

    assert target_file.exists()
    assert target_file.stat().st_size == 0


def test_download_destination_parent_missing(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test behavior when destination directory does not exist."""
    # current implementation just opens file, so it should raise FileNotFoundError (OSError)
    # The current implementation wraps RequestException but not OSError from file opening.
    # Ideally it should probably handle it or let it bubble up as a system error.
    # Let's verify it raises whatever open() raises, or we might want to improve the implementation later.
    # For now, let's just see what happens.

    target_file = tmp_path / "missing_dir" / "test.zip"

    mock_response = MagicMock()
    mock_response.iter_content.return_value = [b"data"]
    mock_response.raise_for_status.return_value = None

    with patch("requests.get", return_value=mock_response) as mock_get:
        mock_get.return_value.__enter__.return_value = mock_response
        with pytest.raises(FileNotFoundError):
            fda_source.download_archive(target_file)


def test_extract_archive_success(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test successful extraction."""
    zip_path = tmp_path / "good.zip"
    extract_dir = tmp_path / "extracted"

    # Create a real zip file
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("test.txt", "hello world")

    files = fda_source.extract_archive(zip_path, extract_dir)

    assert len(files) == 1
    assert files[0].name == "test.txt"
    assert (extract_dir / "test.txt").read_text() == "hello world"


def test_extract_archive_bad_zip(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test extraction of invalid zip raises SourceSchemaError."""
    zip_path = tmp_path / "bad.zip"
    extract_dir = tmp_path / "extracted"

    # Write garbage
    zip_path.write_text("not a zip")

    with pytest.raises(SourceSchemaError, match="not a valid ZIP"):
        fda_source.extract_archive(zip_path, extract_dir)


def test_extract_archive_missing_file(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test extraction of missing file raises SourceConnectionError."""
    zip_path = tmp_path / "missing.zip"
    extract_dir = tmp_path / "extracted"

    with pytest.raises(SourceConnectionError, match="not found"):
        fda_source.extract_archive(zip_path, extract_dir)


def test_extract_archive_zip_slip(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test protection against Zip Slip vulnerability."""
    zip_path = tmp_path / "slip.zip"
    extract_dir = tmp_path / "extracted"
    extract_dir.mkdir()

    # Create a zip with a file attempting to traverse directories
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("../evil.txt", "I am evil")
        zf.writestr("good.txt", "I am good")

    files = fda_source.extract_archive(zip_path, extract_dir)

    # The evil file should NOT be in the returned list and NOT on disk outside
    assert (extract_dir / "good.txt").exists()
    assert not (extract_dir.parent / "evil.txt").exists()

    evil_path = extract_dir.parent / "evil.txt"
    assert evil_path not in files
    assert len(files) == 1


def test_extract_archive_zip_slip_exception(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test protection against Zip Slip vulnerability with exception handling."""
    zip_path = tmp_path / "slip_exception.zip"
    extract_dir = tmp_path / "extracted_exception"
    extract_dir.mkdir()

    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("good.txt", "good")

    # Mock Path.is_relative_to to raise ValueError (simulating cross-drive on Windows or similar)
    with patch("pathlib.Path.is_relative_to", side_effect=ValueError("Test error")):
        files = fda_source.extract_archive(zip_path, extract_dir)
        # Should be empty because it catches the error and skips
        assert len(files) == 0


def test_extract_nested_structure(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test extraction of nested directories."""
    zip_path = tmp_path / "nested.zip"
    extract_dir = tmp_path / "extracted_nested"

    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("folder/subfolder/file.txt", "content")
        zf.writestr("root.txt", "root")

    files = fda_source.extract_archive(zip_path, extract_dir)

    assert len(files) == 2
    assert (extract_dir / "folder/subfolder/file.txt").exists()
    assert (extract_dir / "root.txt").exists()


def test_extract_overwrite_existing(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test that extraction overwrites existing files."""
    zip_path = tmp_path / "overwrite.zip"
    extract_dir = tmp_path / "extracted_overwrite"
    extract_dir.mkdir()

    # Pre-existing file
    target_file = extract_dir / "file.txt"
    target_file.write_text("old content")

    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("file.txt", "new content")

    fda_source.extract_archive(zip_path, extract_dir)

    assert target_file.read_text() == "new content"


def test_extract_empty_zip(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test extraction of a valid empty zip file."""
    zip_path = tmp_path / "empty_valid.zip"
    extract_dir = tmp_path / "extracted_empty"

    with zipfile.ZipFile(zip_path, "w") as _:  # renamed to _
        pass  # Empty zip

    files = fda_source.extract_archive(zip_path, extract_dir)
    assert len(files) == 0


def test_cleanup_file(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test cleanup of a file."""
    f = tmp_path / "temp.txt"
    f.touch()
    assert f.exists()

    fda_source.cleanup(f)
    assert not f.exists()


def test_cleanup_dir(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test cleanup of a directory."""
    d = tmp_path / "tempdir"
    d.mkdir()
    (d / "file.txt").touch()
    assert d.exists()

    fda_source.cleanup(d)
    assert not d.exists()


def test_cleanup_nonexistent(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test cleanup ignores non-existent paths."""
    p = tmp_path / "ghost"
    # Should not raise
    fda_source.cleanup(p)


def test_cleanup_oserror(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test cleanup handling of OSError."""
    f = tmp_path / "temp.txt"
    f.touch()

    with patch("pathlib.Path.unlink", side_effect=OSError("Permission denied")):
        fda_source.cleanup(f)

    assert f.exists()  # Should still exist because unlink failed


def test_calculate_file_hash_success(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test MD5 hash calculation for a known file content."""
    test_file = tmp_path / "hash_test.txt"
    content = b"hello world"
    test_file.write_bytes(content)

    expected_hash = hashlib.md5(content).hexdigest()
    assert fda_source.calculate_file_hash(test_file) == expected_hash


def test_calculate_file_hash_large_file(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test MD5 hash calculation for a file larger than CHUNK_SIZE."""
    test_file = tmp_path / "large_file.txt"
    # Create content larger than 8192 bytes
    content = b"a" * (fda_source.CHUNK_SIZE * 2 + 100)
    test_file.write_bytes(content)

    expected_hash = hashlib.md5(content).hexdigest()
    assert fda_source.calculate_file_hash(test_file) == expected_hash


def test_calculate_file_hash_empty_file(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test MD5 hash calculation for an empty file."""
    test_file = tmp_path / "empty_hash.txt"
    test_file.touch()

    expected_hash = hashlib.md5(b"").hexdigest()
    assert fda_source.calculate_file_hash(test_file) == expected_hash


def test_calculate_file_hash_file_not_found(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test that calculating hash for a non-existent file raises FileNotFoundError."""
    test_file = tmp_path / "non_existent.txt"
    with pytest.raises(FileNotFoundError):
        fda_source.calculate_file_hash(test_file)


def test_calculate_file_hash_oserror(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test robust error handling when file read fails."""
    test_file = tmp_path / "error_file.txt"
    test_file.touch()

    # Mock open to succeed, but read to fail
    with patch("builtins.open", side_effect=OSError("Disk error")):
        with pytest.raises(OSError, match="Disk error"):
            fda_source.calculate_file_hash(test_file)


def test_calculate_file_hash_directory(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test that calculating hash for a directory raises an error."""
    test_dir = tmp_path / "test_dir"
    test_dir.mkdir()

    # On POSIX, open() on a directory might raise IsADirectoryError.
    # On Windows, it might be PermissionError. Both are OSErrors.
    # We expect the method to bubble up the exception.
    with pytest.raises(OSError):
        fda_source.calculate_file_hash(test_dir)


def test_calculate_file_hash_permission_error(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test handling of permission errors."""
    test_file = tmp_path / "protected.txt"
    test_file.touch()

    # Mock open to raise PermissionError
    with patch("builtins.open", side_effect=PermissionError("Access denied")):
        with pytest.raises(PermissionError, match="Access denied"):
            fda_source.calculate_file_hash(test_file)


def test_calculate_file_hash_binary_content(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test hashing of binary non-text content."""
    test_file = tmp_path / "binary.bin"
    # Random bytes including nulls and non-printables
    content = b"\x00\xFF\x10\xCA\xFE\xBA\xBE" * 100
    test_file.write_bytes(content)

    expected_hash = hashlib.md5(content).hexdigest()
    assert fda_source.calculate_file_hash(test_file) == expected_hash
