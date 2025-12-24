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
import pytest
from unittest.mock import patch
from coreason_etl_fda_orange_book.main import parse_args, main
from coreason_etl_fda_orange_book.config import FdaConfig


def test_parse_args_defaults() -> None:
    """Test argument parsing with default values."""
    args = parse_args([])
    assert args.base_url == FdaConfig.DEFAULT_BASE_URL


def test_parse_args_custom_url() -> None:
    """Test argument parsing with a custom base URL."""
    custom_url = "https://example.com/data.zip"
    args = parse_args(["--base-url", custom_url])
    assert args.base_url == custom_url


def test_main_execution(capsys: pytest.CaptureFixture[str]) -> None:
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
