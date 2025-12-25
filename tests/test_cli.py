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

import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from coreason_etl_fda_orange_book.config import FdaConfig
from coreason_etl_fda_orange_book.main import main, parse_args, setup_logging


def test_parse_args_defaults() -> None:
    """Test argument parsing with default values."""
    args = parse_args([])
    assert args.base_url == FdaConfig.DEFAULT_BASE_URL
    assert args.download_dir == FdaConfig.DEFAULT_DOWNLOAD_DIR


def test_parse_args_custom_url() -> None:
    """Test argument parsing with a custom base URL."""
    custom_url = "https://example.com/data.zip"
    args = parse_args(["--base-url", custom_url])
    assert args.base_url == custom_url


def test_parse_args_custom_download_dir() -> None:
    """Test argument parsing with a custom download directory."""
    custom_dir = Path("/tmp/custom_fda")
    args = parse_args(["--download-dir", str(custom_dir)])
    assert args.download_dir == custom_dir


def test_parse_args_unknown_arg() -> None:
    """Test that unknown arguments cause the parser to exit."""
    with pytest.raises(SystemExit) as excinfo:
        parse_args(["--unknown-arg"])
    assert excinfo.value.code == 2


def test_parse_args_help(capsys: pytest.CaptureFixture[str]) -> None:
    """Test that the help flag works."""
    with pytest.raises(SystemExit) as excinfo:
        parse_args(["--help"])
    assert excinfo.value.code == 0
    captured = capsys.readouterr()
    assert "FDA Orange Book ETL Pipeline" in captured.out
    assert "--base-url" in captured.out
    assert "--download-dir" in captured.out


def test_setup_logging_default() -> None:
    """Test logging setup with default level."""
    with patch("coreason_etl_fda_orange_book.main.logger") as mock_logger:
        setup_logging()
        mock_logger.remove.assert_called_once()
        mock_logger.add.assert_called_once_with(sys.stderr, level="INFO")


def test_setup_logging_env_var() -> None:
    """Test logging setup with LOG_LEVEL environment variable."""
    with (
        patch.dict(os.environ, {"LOG_LEVEL": "DEBUG"}),
        patch("coreason_etl_fda_orange_book.main.logger") as mock_logger,
    ):
        setup_logging()
        mock_logger.add.assert_called_once_with(sys.stderr, level="DEBUG")


def test_setup_logging_invalid_level() -> None:
    """
    Test logging setup with an invalid LOG_LEVEL.

    Loguru's behavior on invalid level depends on implementation details,
    but typically it might raise an error or fallback. Since we pass the string
    directly to `logger.add`, loguru validation logic applies.
    We are just testing that we pass the env var value correctly.
    """
    with (
        patch.dict(os.environ, {"LOG_LEVEL": "INVALID_LEVEL"}),
        patch("coreason_etl_fda_orange_book.main.logger") as mock_logger,
    ):
        setup_logging()
        mock_logger.add.assert_called_once_with(sys.stderr, level="INVALID_LEVEL")


def test_main_execution() -> None:
    """Test main execution flow."""
    with patch("coreason_etl_fda_orange_book.main.logger") as mock_logger:
        main([])

        # Verify logger calls (checking functionality, not just output)
        assert mock_logger.info.call_count >= 2
        mock_logger.info.assert_any_call("Starting FDA Orange Book ETL Pipeline")


def test_main_with_args() -> None:
    """Test main execution with arguments."""
    custom_url = "https://example.com/test.zip"
    custom_dir = Path("/tmp/test_dir")
    with patch("coreason_etl_fda_orange_book.main.logger") as mock_logger:
        main(["--base-url", custom_url, "--download-dir", str(custom_dir)])
        mock_logger.info.assert_any_call(f"Using Base URL: {custom_url}")
        mock_logger.info.assert_any_call(f"Download Directory: {custom_dir}")


def test_main_implicit_args() -> None:
    """Test main execution when implicit args (sys.argv) are used or empty list passed."""
    with patch.object(sys, "argv", ["program_name"]), patch("coreason_etl_fda_orange_book.main.logger") as mock_logger:
        main(None)
        mock_logger.info.assert_any_call("Starting FDA Orange Book ETL Pipeline")
