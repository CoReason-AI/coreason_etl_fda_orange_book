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

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from coreason_etl_fda_orange_book.main import main, parse_args, setup_logging
from coreason_etl_fda_orange_book.config import FdaConfig


def test_parse_args_defaults() -> None:
    """Test argument parsing with default values."""
    args = parse_args([])
    assert args.base_url == FdaConfig.DEFAULT_BASE_URL
    assert args.download_dir == FdaConfig.DEFAULT_DOWNLOAD_DIR


def test_parse_args_custom() -> None:
    """Test argument parsing with custom values."""
    custom_url = "http://example.com/zip"
    custom_dir = "/tmp/custom"
    args = parse_args(["--base-url", custom_url, "--download-dir", custom_dir])
    assert args.base_url == custom_url
    assert args.download_dir == Path(custom_dir)


def test_setup_logging(capsys: pytest.CaptureFixture[str]) -> None:
    """Test logging setup."""
    # This is hard to test side effects of loguru, but we can check it runs without error
    # and maybe check env var handling.
    with patch.dict("os.environ", {"LOG_LEVEL": "DEBUG"}):
        setup_logging()

    # We can't easily assert loguru output to stderr with capsys because loguru handles it specially,
    # but ensuring it doesn't crash is a baseline.


def test_main_execution() -> None:
    """Test the main function runs through."""
    with patch("coreason_etl_fda_orange_book.main.logger") as mock_logger:
        main([])

        # Verify logger calls
        mock_logger.info.assert_any_call("Starting FDA Orange Book ETL Pipeline")
        mock_logger.info.assert_any_call("Pipeline initialized successfully.")


def test_main_with_args() -> None:
    """Test main with arguments."""
    with patch("coreason_etl_fda_orange_book.main.logger") as mock_logger:
        main(["--base-url", "http://test.com"])
        mock_logger.info.assert_any_call("Using Base URL: http://test.com")
