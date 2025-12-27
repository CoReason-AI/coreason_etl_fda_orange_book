# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Advanced ingestion edge case tests for Bronze layer."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from coreason_etl_fda_orange_book.bronze.ingestion import yield_bronze_records
from coreason_etl_fda_orange_book.source import FdaOrangeBookSource


class TestBronzeIngestionAdvanced:
    """Test advanced ingestion scenarios like encoding and file anomalies."""

    @pytest.fixture(name="source")
    def source(self) -> FdaOrangeBookSource:
        """Mock source."""
        source = MagicMock(spec=FdaOrangeBookSource)
        source.calculate_file_hash.return_value = "mock_hash"
        return source

    def test_ingestion_invalid_encoding(self, source: FdaOrangeBookSource, tmp_path: Path) -> None:
        """
        Test that files with invalid UTF-8 bytes are read with replacement characters
        instead of crashing, ensuring resilience.
        """
        file_path = tmp_path / "invalid_utf8.txt"
        # 0x80 is invalid as a start byte in UTF-8
        file_path.write_bytes(b"Valid data\nInvalid \x80 byte\nMore data")

        files_map = {"products": [file_path]}
        records = list(yield_bronze_records(files_map, source))

        assert len(records) == 3
        assert records[0]["raw_content"]["data"] == "Valid data"
        # The invalid byte should be replaced by replacement char (U+FFFD)
        assert "Invalid \ufffd byte" in records[1]["raw_content"]["data"]
        assert records[2]["raw_content"]["data"] == "More data"

    def test_ingestion_zero_byte_file(self, source: FdaOrangeBookSource, tmp_path: Path) -> None:
        """Test that ingestion handles a 0-byte file gracefully."""
        file_path = tmp_path / "empty.txt"
        file_path.write_text("")

        files_map = {"products": [file_path]}
        records = list(yield_bronze_records(files_map, source))

        assert len(records) == 0

    def test_ingestion_long_line(self, source: FdaOrangeBookSource, tmp_path: Path) -> None:
        """Test that ingestion handles very long lines (e.g. 1MB)."""
        file_path = tmp_path / "long_line.txt"
        long_string = "a" * 1024 * 1024  # 1MB
        file_path.write_text(f"short line\n{long_string}\nshort line 2")

        files_map = {"products": [file_path]}
        records = list(yield_bronze_records(files_map, source))

        assert len(records) == 3
        assert len(records[1]["raw_content"]["data"]) == 1024 * 1024
