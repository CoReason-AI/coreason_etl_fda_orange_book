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
    # Note: python's zipfile module might sanitize this by default or raise a warning,
    # but we want to ensure our logic handles/skips it.
    with zipfile.ZipFile(zip_path, "w") as zf:
        # We manually add a file with a path traversal name
        # Using writestr with a specific arcname
        zf.writestr("../evil.txt", "I am evil")
        zf.writestr("good.txt", "I am good")

    files = fda_source.extract_archive(zip_path, extract_dir)

    # The evil file should NOT be in the returned list and NOT on disk outside
    assert (extract_dir / "good.txt").exists()
    assert not (extract_dir.parent / "evil.txt").exists()

    # The implementation might skip it or sanitize it.
    # Our implementation skips it if it's not relative to destination.

    # Check that the evil file is NOT in the returned list
    # The returned list contains full paths.
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
