# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Tests for source module error handling."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# UPDATED: Use curl_cffi
from curl_cffi import requests

from coreason_etl_fda_orange_book.exceptions import SourceConnectionError, SourceSchemaError
from coreason_etl_fda_orange_book.source import FdaOrangeBookSource


class TestSourceErrors:
    """Test specific error handling scenarios."""

    @pytest.fixture(name="source")
    def source(self) -> FdaOrangeBookSource:
        """Fixture for source."""
        return FdaOrangeBookSource()

    def test_download_404_raises_schema_error(self, source: FdaOrangeBookSource, tmp_path: Path) -> None:
        """Test that HTTP 404 raises SourceSchemaError."""
        target = tmp_path / "test.zip"

        # Mock 404 response
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        # We don't raise Exception here, the code checks status_code manually
        # But we need to make sure response.url doesn't trigger abuse check
        mock_resp.url = "http://clean.url"

        with patch("curl_cffi.requests.get", return_value=mock_resp) as mock_get:
            mock_get.return_value.__enter__.return_value = mock_resp
            with pytest.raises(SourceSchemaError, match="Download link not found"):
                source.download_archive(target)

    def test_download_500_raises_connection_error(self, source: FdaOrangeBookSource, tmp_path: Path) -> None:
        """Test that HTTP 500 raises SourceConnectionError."""
        target = tmp_path / "test.zip"

        # Mock 500 response
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.url = "http://clean.url"
        # raise_for_status raises RequestsError in curl_cffi?
        # Let's mock raise_for_status to raise RequestsError
        mock_resp.raise_for_status.side_effect = requests.RequestsError("500 Server Error")

        with patch("curl_cffi.requests.get", return_value=mock_resp) as mock_get:
            mock_get.return_value.__enter__.return_value = mock_resp
            with pytest.raises(SourceConnectionError, match="Failed to download"):
                source.download_archive(target)

    def test_download_other_request_exception(self, source: FdaOrangeBookSource, tmp_path: Path) -> None:
        """Test generic request exception raises SourceConnectionError."""
        target = tmp_path / "test.zip"

        with patch("curl_cffi.requests.get", side_effect=requests.RequestsError("Connection refused")):
            with pytest.raises(SourceConnectionError, match="Failed to download"):
                source.download_archive(target)
