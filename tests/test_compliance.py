"""
Tests for GxP Compliance, Audit Logs, and PII Protection.
"""
import os
import json
import logging
import pytest
from pathlib import Path
from loguru import logger
from unittest.mock import patch, MagicMock

# Import the logger config to verify PII masking
from coreason_etl_fda_orange_book.utils.logger import mask_pii, audit_context

def test_pii_masking():
    """Verify that PII patterns are masked in logs."""
    record = {"message": "User email is jules@example.com"}
    mask_pii(record)
    assert "[MASKED_EMAIL]" in record["message"]
    assert "jules@example.com" not in record["message"]

    record = {"message": "SSN: 123-45-6789"}
    mask_pii(record)
    assert "[MASKED_SSN]" in record["message"]
    assert "123-45-6789" not in record["message"]

    record = {"message": "CC: 1234-5678-9012-3456"}
    mask_pii(record)
    assert "[MASKED_CC]" in record["message"]
    assert "1234-5678-9012-3456" not in record["message"]

def test_audit_context_injection():
    """Verify that user and environment are injected into log records."""
    record = {"extra": {}}

    with patch.dict(os.environ, {"USER": "test_auditor", "APP_ENV": "compliance_test"}):
        audit_context(record)
        assert record["extra"]["user"] == "test_auditor"
        assert record["extra"]["environment"] == "compliance_test"

def test_bronze_ingestion_no_silent_failure():
    """Verify that bronze ingestion raises exception on error instead of silencing it."""
    from coreason_etl_fda_orange_book.bronze.ingestion import yield_bronze_records

    # Mock files_map and source
    files_map = {"test_role": [Path("non_existent_file.txt")]}
    source_mock = MagicMock()
    # Simulate hash calculation failure
    source_mock.calculate_file_hash.side_effect = Exception("Hash Failed")

    # It should raise the exception now
    with pytest.raises(Exception, match="Hash Failed"):
        list(yield_bronze_records(files_map, source_mock))

def test_silver_transform_no_silent_failure():
    """Verify that silver transformation raises exception on error."""
    from coreason_etl_fda_orange_book.silver.transform import _clean_read_csv

    # Pass a path that causes read_csv to fail (or mock pl.read_csv)
    # Here we rely on the fact that the file doesn't exist and pl.read_csv should fail
    # or we can mock it.

    with patch("polars.read_csv", side_effect=Exception("Read Failed")):
        with pytest.raises(Exception, match="Read Failed"):
            _clean_read_csv(Path("dummy.csv"))
