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
import requests

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
        error = requests.HTTPError("404 Client Error: Not Found", response=mock_resp)

        with patch("requests.get", side_effect=error):
            with pytest.raises(SourceSchemaError, match="Download link not found"):
                source.download_archive(target)

    def test_download_500_raises_connection_error(self, source: FdaOrangeBookSource, tmp_path: Path) -> None:
        """Test that HTTP 500 raises SourceConnectionError."""
        target = tmp_path / "test.zip"

        # Mock 500 response
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        error = requests.HTTPError("500 Server Error", response=mock_resp)

        with patch("requests.get", side_effect=error):
            with pytest.raises(SourceConnectionError, match="HTTP error downloading"):
                source.download_archive(target)

    def test_download_other_request_exception(self, source: FdaOrangeBookSource, tmp_path: Path) -> None:
        """Test generic request exception raises SourceConnectionError."""
        target = tmp_path / "test.zip"

        with patch("requests.get", side_effect=requests.RequestException("Connection refused")):
            with pytest.raises(SourceConnectionError, match="Failed to download"):
                source.download_archive(target)
