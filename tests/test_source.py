# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Tests for source module."""

import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from coreason_etl_fda_orange_book.config import FdaConfig
from coreason_etl_fda_orange_book.exceptions import SourceConnectionError, SourceSchemaError
from coreason_etl_fda_orange_book.source import FdaOrangeBookSource


@pytest.fixture(name="fda_source")
def fda_source() -> FdaOrangeBookSource:
    """Fixture for FdaOrangeBookSource instance."""
    return FdaOrangeBookSource()


def test_init_defaults(fda_source: FdaOrangeBookSource) -> None:
    """Test default initialization."""
    assert fda_source.base_url == FdaConfig.DEFAULT_BASE_URL


def test_init_custom() -> None:
    """Test custom initialization."""
    url = "http://test.com/zip"
    source = FdaOrangeBookSource(base_url=url)
    assert source.base_url == url


def test_download_archive_success(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test successful download."""
    target_file = tmp_path / "test.zip"

    # Mock response
    mock_response = MagicMock()
    mock_response.iter_content.return_value = [b"chunk1", b"chunk2"]
    mock_response.raise_for_status.return_value = None

    with patch("requests.get", return_value=mock_response) as mock_get:
        # Need to support context manager
        mock_get.return_value.__enter__.return_value = mock_response
        fda_source.download_archive(target_file)

        mock_get.assert_called_with(fda_source.base_url, stream=True, timeout=60)
        assert target_file.exists()
        assert target_file.read_bytes() == b"chunk1chunk2"


def test_download_archive_failure(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test download failure."""
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
    """Test successful hash calculation."""
    f = tmp_path / "test.txt"
    f.write_text("hello", encoding="utf-8")

    # MD5 of "hello" is 5d41402abc4b2a76b9719d911017c592
    assert fda_source.calculate_file_hash(f) == "5d41402abc4b2a76b9719d911017c592"


def test_calculate_file_hash_missing_file(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test hash calculation fails for missing file."""
    f = tmp_path / "missing.txt"
    with pytest.raises(SourceConnectionError, match="File not found"):
        fda_source.calculate_file_hash(f)


def test_calculate_file_hash_oserror(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test hash calculation handles OSError."""
    f = tmp_path / "test.txt"
    f.touch()

    with patch("builtins.open", side_effect=OSError("Read error")):
        with pytest.raises(SourceConnectionError, match="Failed to calculate hash"):
            fda_source.calculate_file_hash(f)


def test_resolve_product_files_success(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test successful resolution of product files."""
    files = [tmp_path / "products.txt", tmp_path / "patent.txt", tmp_path / "exclusivity.txt"]
    mapping = fda_source.resolve_product_files(files)
    assert len(mapping["products"]) == 1
    assert mapping["products"][0].name == "products.txt"
    assert len(mapping["patent"]) == 1
    assert len(mapping["exclusivity"]) == 1


def test_resolve_product_files_fallback(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test fallback to rx/otc/disc files."""
    files = [tmp_path / "rx.txt", tmp_path / "otc.txt", tmp_path / "patent.txt"]
    mapping = fda_source.resolve_product_files(files)
    assert len(mapping["products"]) == 2
    names = sorted([p.name for p in mapping["products"]])
    assert names == ["otc.txt", "rx.txt"]


def test_resolve_product_files_missing_products(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test failure when product files are missing."""
    files = [tmp_path / "patent.txt"]
    with pytest.raises(SourceSchemaError, match="Missing required product files"):
        fda_source.resolve_product_files(files)


def test_resolve_product_files_missing_optional(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test missing optional files (patent, exclusivity) logs warning but doesn't raise."""
    files = [tmp_path / "products.txt"]
    mapping = fda_source.resolve_product_files(files)
    assert len(mapping["patent"]) == 0
    assert len(mapping["exclusivity"]) == 0
