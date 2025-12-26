# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Tests for the CLI entry point."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from coreason_etl_fda_orange_book.main import main, parse_args


class TestCli:
    """Tests for CLI arguments and execution."""

    def test_parse_args_defaults(self) -> None:
        """Test default arguments."""
        args = parse_args([])
        assert args.base_url == "https://www.fda.gov/media/76860/download?attachment"
        assert args.download_dir == Path("data/bronze")

    def test_parse_args_custom(self) -> None:
        """Test custom arguments."""
        args = parse_args(["--base-url", "http://test.com/zip", "--download-dir", "/tmp/test"])
        assert args.base_url == "http://test.com/zip"
        assert args.download_dir == Path("/tmp/test")

    @patch("coreason_etl_fda_orange_book.main.run_pipeline")
    def test_main_success(self, mock_run: MagicMock) -> None:
        """Test successful main execution."""
        main([])
        mock_run.assert_called_once()

    @patch("coreason_etl_fda_orange_book.main.run_pipeline")
    def test_main_failure(self, mock_run: MagicMock) -> None:
        """Test failure handling in main."""
        mock_run.side_effect = Exception("Boom")
        with pytest.raises(SystemExit):
            main([])

    @patch("coreason_etl_fda_orange_book.main.dlt.pipeline")
    @patch("coreason_etl_fda_orange_book.main.FdaOrangeBookSource")
    def test_run_pipeline_flow(self, mock_source_cls: MagicMock, mock_pipeline: MagicMock, tmp_path: Path) -> None:
        """Test the run_pipeline orchestration logic (mocked)."""
        from coreason_etl_fda_orange_book.main import run_pipeline

        # Mock Source
        mock_source = mock_source_cls.return_value
        mock_source.extract_archive.return_value = []
        mock_source.resolve_product_files.return_value = {"products": [], "patent": [], "exclusivity": []}

        # Mock DLT Pipeline
        mock_pipe_instance = mock_pipeline.return_value
        mock_pipe_instance.run.return_value = "LoadInfo"

        run_pipeline("http://test", tmp_path)

        # Check calls
        mock_source.download_archive.assert_called_once()
        mock_source.extract_archive.assert_called_once()
        mock_source.resolve_product_files.assert_called_once()

        # Should be called 3 times (Bronze, Silver, Gold)
        assert mock_pipeline.call_count == 3
        assert mock_pipe_instance.run.call_count == 3
