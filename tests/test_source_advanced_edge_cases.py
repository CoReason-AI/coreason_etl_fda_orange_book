# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Advanced edge case tests for source module (Network & Contract)."""

import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from coreason_etl_fda_orange_book.exceptions import SourceConnectionError, SourceSchemaError
from coreason_etl_fda_orange_book.source import FdaOrangeBookSource


class TestSourceNetworkResilience:
    """Test network unavailability and weird HTTP states."""

    @pytest.fixture(name="source")
    def source(self) -> FdaOrangeBookSource:
        """Fixture for source."""
        return FdaOrangeBookSource()

    def test_download_timeout(self, source: FdaOrangeBookSource, tmp_path: Path) -> None:
        """Test that connection timeout raises SourceConnectionError."""
        target = tmp_path / "timeout.zip"

        # requests.Timeout is a subclass of RequestException
        with patch("requests.get", side_effect=requests.Timeout("Connection timed out")):
            with pytest.raises(SourceConnectionError, match="Failed to download"):
                source.download_archive(target)

    def test_download_dns_failure(self, source: FdaOrangeBookSource, tmp_path: Path) -> None:
        """Test that DNS failure (ConnectionError) raises SourceConnectionError."""
        target = tmp_path / "dns.zip"

        # requests.ConnectionError is raised for DNS/Refused errors
        with patch("requests.get", side_effect=requests.ConnectionError("Name or service not known")):
            with pytest.raises(SourceConnectionError, match="Failed to download"):
                source.download_archive(target)

    def test_download_rate_limit_429(self, source: FdaOrangeBookSource, tmp_path: Path) -> None:
        """Test that HTTP 429 (Too Many Requests) raises SourceConnectionError."""
        target = tmp_path / "ratelimit.zip"

        mock_resp = MagicMock()
        mock_resp.status_code = 429
        # raise_for_status raises HTTPError
        error = requests.HTTPError("429 Client Error: Too Many Requests", response=mock_resp)

        with patch("requests.get", side_effect=error):
            # Currently the code catches generic HTTPError/RequestException
            # It should wrap it in SourceConnectionError
            with pytest.raises(SourceConnectionError, match="HTTP error downloading"):
                source.download_archive(target)

    def test_download_redirect_302(self, source: FdaOrangeBookSource, tmp_path: Path) -> None:
        """Test that 302 Redirect is followed (success)."""
        target = tmp_path / "redirect.zip"

        # Mock a successful response that was redirected
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.history = [MagicMock(status_code=302)]
        mock_resp.iter_content = lambda chunk_size: [b"redirected_content"]
        mock_resp.__enter__.return_value = mock_resp
        mock_resp.__exit__.return_value = None

        with patch("requests.get", return_value=mock_resp):
            source.download_archive(target)

            # Verify file content
            assert target.exists()
            assert target.read_bytes() == b"redirected_content"


class TestSourceContractEdgeCases:
    """Test source file structure/contract anomalies."""

    @pytest.fixture(name="source")
    def source(self) -> FdaOrangeBookSource:
        return FdaOrangeBookSource()

    def test_zip_empty_valid(self, source: FdaOrangeBookSource, tmp_path: Path) -> None:
        """Test a valid ZIP file that contains no files."""
        zip_path = tmp_path / "empty.zip"
        dest_dir = tmp_path / "extracted_empty"
        dest_dir.mkdir()

        # Create empty zip
        with zipfile.ZipFile(zip_path, "w"):
            pass

        extracted = source.extract_archive(zip_path, dest_dir)
        assert extracted == []

        # resolve_product_files should fail because products.txt is missing
        with pytest.raises(SourceSchemaError, match="Missing required product files"):
            source.resolve_product_files(extracted)

    def test_zip_nested_folders(self, source: FdaOrangeBookSource, tmp_path: Path) -> None:
        """Test that files inside nested folders in ZIP are handled."""
        zip_path = tmp_path / "nested.zip"
        dest_dir = tmp_path / "extracted_nested"
        dest_dir.mkdir()

        # Create zip with nested structure
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("nested/folder/products.txt", "content")
            zf.writestr("patent.txt", "content")  # Root level

        extracted = source.extract_archive(zip_path, dest_dir)

        # Verify extraction structure
        assert (dest_dir / "nested/folder/products.txt").exists()
        assert (dest_dir / "patent.txt").exists()

        # Verify resolution finds products.txt even if nested?
        # The current implementation checks `f.name` which is the basename.
        # "products.txt" should match "nested/folder/products.txt" if we look at .name
        mapping = source.resolve_product_files(extracted)
        assert len(mapping["products"]) == 1
        assert mapping["products"][0].name == "products.txt"

    def test_zip_missing_required(self, source: FdaOrangeBookSource, tmp_path: Path) -> None:
        """Test missing critical files raises SchemaError."""
        zip_path = tmp_path / "incomplete.zip"
        dest_dir = tmp_path / "extracted_incomplete"
        dest_dir.mkdir()

        # Zip with only patent
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("patent.txt", "content")

        extracted = source.extract_archive(zip_path, dest_dir)

        with pytest.raises(SourceSchemaError, match="Missing required product files"):
            source.resolve_product_files(extracted)
