# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Tests for pipeline cleanup logic."""

from pathlib import Path
from unittest.mock import MagicMock, patch

from coreason_etl_fda_orange_book.main import run_pipeline


def test_pipeline_cleanup_on_success(tmp_path: Path) -> None:
    """Test cleanup happens after successful run."""
    download_dir = tmp_path / "bronze"
    download_dir.mkdir()

    # Mock source to verify cleanup called
    with patch("coreason_etl_fda_orange_book.main.FdaOrangeBookSource") as MockSource:
        mock_instance = MockSource.return_value
        mock_instance.extract_archive.return_value = []
        mock_instance.resolve_product_files.return_value = {}

        # Mock dlt pipeline run to avoid real DB connection
        with patch("dlt.pipeline") as mock_pipeline:
            run_pipeline("http://test.com", download_dir)

            # Verify cleanup called
            mock_instance.cleanup.assert_called_once_with(download_dir)


def test_pipeline_cleanup_on_failure(tmp_path: Path) -> None:
    """Test cleanup happens after failed run."""
    download_dir = tmp_path / "bronze"

    with patch("coreason_etl_fda_orange_book.main.FdaOrangeBookSource") as MockSource:
        mock_instance = MockSource.return_value
        # Simulate failure during download
        mock_instance.download_archive.side_effect = Exception("Download failed")

        try:
            run_pipeline("http://test.com", download_dir)
        except Exception:
            pass

        # Verify cleanup called despite exception
        mock_instance.cleanup.assert_called_once_with(download_dir)
