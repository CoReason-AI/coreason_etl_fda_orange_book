# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Integration tests for the Bronze layer workflow."""

import hashlib
import zipfile
from pathlib import Path

import pytest

from coreason_etl_fda_orange_book.bronze.ingestion import yield_bronze_records
from coreason_etl_fda_orange_book.source import FdaOrangeBookSource


def test_bronze_workflow_integration(tmp_path: Path) -> None:
    """
    Test the full Bronze layer workflow: Extraction -> Mapping -> Ingestion.

    This test verifies that:
    1. A ZIP file can be extracted.
    2. The extracted files are correctly mapped (handling split products).
    3. The ingestion generator yields correct records with valid hashes.
    """
    # 1. Setup: Create a source ZIP file
    zip_path = tmp_path / "source.zip"
    extract_dir = tmp_path / "bronze_raw"
    extract_dir.mkdir()

    rx_content = "Appl_No~Product_No~Type\n001234~001~RX"
    otc_content = "Appl_No~Product_No~Type\n005678~001~OTC"
    patent_content = "Appl_No~Product_No~Patent_No\n001234~001~1234567"

    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("rx.txt", rx_content)
        zf.writestr("otc.txt", otc_content)
        zf.writestr("patent.txt", patent_content)
        # Add a junk file that should be ignored by mapping (unless we mapped it?)
        # Currently we only map products, patent, exclusivity.
        zf.writestr("readme.txt", "Read me")

    # 2. Execution
    source = FdaOrangeBookSource()

    # Step A: Extract
    extracted_files = source.extract_archive(zip_path, extract_dir)
    assert len(extracted_files) == 4

    # Step B: Map
    files_map = source.resolve_product_files(extracted_files)

    # Verify mapping logic (Split products logic)
    assert "products" in files_map
    product_files = sorted([f.name for f in files_map["products"]])
    assert product_files == ["otc.txt", "rx.txt"]

    assert "patent" in files_map
    assert files_map["patent"][0].name == "patent.txt"

    # Step C: Ingest
    records = list(yield_bronze_records(files_map, source))

    # 3. Verification
    assert len(records) == 6  # 2 lines RX + 2 lines OTC + 2 lines Patent

    # Verify content and hashes
    rx_hash = hashlib.md5(rx_content.encode("utf-8")).hexdigest()

    rx_records = [r for r in records if r["source_file"] == "rx.txt"]
    assert len(rx_records) == 2
    assert rx_records[0]["source_hash"] == rx_hash
    assert rx_records[0]["raw_content"]["data"] == "Appl_No~Product_No~Type"
    assert rx_records[1]["raw_content"]["data"] == "001234~001~RX"
    assert rx_records[0]["role"] == "products"

    otc_records = [r for r in records if r["source_file"] == "otc.txt"]
    assert len(otc_records) == 2
    assert otc_records[0]["role"] == "products"

    patent_records = [r for r in records if r["source_file"] == "patent.txt"]
    assert len(patent_records) == 2
    assert patent_records[0]["role"] == "patent"
