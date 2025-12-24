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
from unittest.mock import patch

import pytest

from coreason_etl_fda_orange_book.config import FdaConfig
from coreason_etl_fda_orange_book.main import main, parse_args, setup_logging


def test_parse_args_defaults() -> None:
    """Test argument parsing with default values."""
    args = parse_args([])
    assert args.base_url == FdaConfig.DEFAULT_BASE_URL


def test_parse_args_custom_url() -> None:
    """Test argument parsing with a custom base URL."""
    custom_url = "https://example.com/data.zip"
    args = parse_args(["--base-url", custom_url])
    assert args.base_url == custom_url


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
    with patch("coreason_etl_fda_orange_book.main.logger") as mock_logger:
        main(["--base-url", custom_url])
        mock_logger.info.assert_any_call(f"Using Base URL: {custom_url}")


def test_main_implicit_args() -> None:
    """Test main execution when implicit args (sys.argv) are used or empty list passed."""
    # When main(None) is called, argparse uses sys.argv.
    # We need to mock sys.argv or ensure it doesn't have conflicting args from pytest.
    # To be safe, we will pass an empty list which mimics 'no args provided' behavior
    # for our parse_args implementation if we were calling it directly, but main(None) triggers sys.argv lookup.

    # However, our main implementation calls `parse_args(args)`.
    # default definition: parse_args(args: list[str] | None = None) -> ... return parser.parse_args(args)
    # If args is None, argparse uses sys.argv[1:].

    # Let's explicitly test main(None) but mock sys.argv to be safe.
    with patch.object(sys, "argv", ["program_name"]), patch("coreason_etl_fda_orange_book.main.logger") as mock_logger:
        main(None)
        mock_logger.info.assert_any_call("Starting FDA Orange Book ETL Pipeline")
