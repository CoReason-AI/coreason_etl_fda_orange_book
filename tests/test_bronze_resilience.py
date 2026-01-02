# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Resilience and edge case tests for Bronze layer (Memory & IO)."""

from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# UPDATED: Use curl_cffi
from curl_cffi import requests

from coreason_etl_fda_orange_book.bronze.ingestion import yield_bronze_records
from coreason_etl_fda_orange_book.exceptions import SourceConnectionError
from coreason_etl_fda_orange_book.source import FdaOrangeBookSource


def test_ingestion_memory_efficiency(tmp_path: Path) -> None:
    """
    Test that ingestion processes files lazily and doesn't load everything into memory.
    Simulates an infinite file.
    """
    # Create a mock source that returns a dummy hash
    mock_source = MagicMock(spec=FdaOrangeBookSource)
    mock_source.calculate_file_hash.return_value = "mock_hash"

    file_path = tmp_path / "infinite.txt"
    files_map = {"test": [file_path]}

    # Create a generator that simulates an infinite file
    def infinite_lines() -> Iterator[str]:
        while True:
            yield "infinite_line_data\n"

    # Mock open() to return our infinite generator
    # We need to mock the context manager structure: with open(...) as f:
    mock_file = MagicMock()
    mock_file.__enter__.return_value = infinite_lines()
    mock_file.__exit__.return_value = None

    with patch("builtins.open", return_value=mock_file):
        # Create the generator from our function
        record_generator = yield_bronze_records(files_map, mock_source)

        # Consume a limited number of records
        # If the code tried to read the whole file (e.g., readlines()), this would hang or OOM.
        for i, record in enumerate(record_generator):
            assert record["raw_content"]["data"] == "infinite_line_data"
            if i >= 100:
                break


def test_download_partial_failure(tmp_path: Path) -> None:
    """
    Test that download_archive handles connection drops mid-stream.
    """
    source = FdaOrangeBookSource()
    dest = tmp_path / "partial.zip"

    # Mock response object
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.status_code = 200
    mock_response.url = "http://clean.url"

    # Simulate partial content: yields one chunk, then raises ConnectionError
    def failing_iter_content(chunk_size: int = 8192) -> Iterator[bytes]:
        yield b"some_data"
        # curl_cffi raises RequestsError for connection issues
        raise requests.RequestsError("Connection drop")

    mock_response.iter_content = failing_iter_content

    with patch("curl_cffi.requests.get", return_value=mock_response):
        # We assume the mocked requests.get is used as a context manager in the source code
        mock_response.__enter__.return_value = mock_response
        mock_response.__exit__.return_value = None

        with pytest.raises(SourceConnectionError, match="Failed to download"):
            source.download_archive(dest)


def test_ingestion_io_interruption(tmp_path: Path) -> None:
    """
    Test that ingestion handles IO errors during file reading (e.g. network drive disconnect).
    """
    mock_source = MagicMock(spec=FdaOrangeBookSource)
    mock_source.calculate_file_hash.return_value = "mock_hash"

    file_path = tmp_path / "broken.txt"
    files_map = {"test": [file_path]}

    # Generator that yields one line then raises OSError
    def broken_lines() -> Iterator[str]:
        yield "line1\n"
        raise OSError("Input/output error")

    mock_file = MagicMock()
    mock_file.__enter__.return_value = broken_lines()
    mock_file.__exit__.return_value = None

    with patch("builtins.open", return_value=mock_file):
        with pytest.raises(OSError, match="Input/output error"):
             list(yield_bronze_records(files_map, mock_source))
