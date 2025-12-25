# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Tests for file detection and mapping logic in FdaOrangeBookSource."""

from pathlib import Path

import pytest

from coreason_etl_fda_orange_book.exceptions import SourceSchemaError
from coreason_etl_fda_orange_book.source import FdaOrangeBookSource


@pytest.fixture
def fda_source() -> FdaOrangeBookSource:
    """Fixture for FdaOrangeBookSource."""
    return FdaOrangeBookSource()


def test_resolve_standard_files(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test resolution when standard products.txt exists."""
    files = [
        tmp_path / "products.txt",
        tmp_path / "patent.txt",
        tmp_path / "exclusivity.txt",
    ]
    # Create empty files
    for f in files:
        f.touch()

    mapping = fda_source.resolve_product_files(files)

    assert len(mapping["products"]) == 1
    assert mapping["products"][0].name == "products.txt"
    assert len(mapping["patent"]) == 1
    assert mapping["patent"][0].name == "patent.txt"
    assert len(mapping["exclusivity"]) == 1
    assert mapping["exclusivity"][0].name == "exclusivity.txt"


def test_resolve_split_products(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test resolution when products are split into components."""
    files = [
        tmp_path / "rx.txt",
        tmp_path / "otc.txt",
        tmp_path / "disc.txt",
        tmp_path / "patent.txt",
    ]
    for f in files:
        f.touch()

    mapping = fda_source.resolve_product_files(files)

    assert len(mapping["products"]) == 3
    product_names = sorted([f.name for f in mapping["products"]])
    assert product_names == ["disc.txt", "otc.txt", "rx.txt"]
    assert len(mapping["patent"]) == 1


def test_resolve_case_insensitive(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test case-insensitive file resolution."""
    files = [
        tmp_path / "PRODUCTS.txt",
        tmp_path / "PATENT.txt",
    ]
    for f in files:
        f.touch()

    mapping = fda_source.resolve_product_files(files)

    assert len(mapping["products"]) == 1
    assert mapping["products"][0].name == "PRODUCTS.txt"
    assert len(mapping["patent"]) == 1


def test_resolve_partial_components(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test resolution with only some component files."""
    files = [
        tmp_path / "rx.txt",
        # Missing otc and disc
    ]
    for f in files:
        f.touch()

    mapping = fda_source.resolve_product_files(files)
    assert len(mapping["products"]) == 1
    assert mapping["products"][0].name == "rx.txt"


def test_resolve_missing_critical(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test error when no product files are found."""
    files = [
        tmp_path / "patent.txt",
    ]
    for f in files:
        f.touch()

    with pytest.raises(SourceSchemaError, match="Missing required product files"):
        fda_source.resolve_product_files(files)


def test_resolve_missing_optional(fda_source: FdaOrangeBookSource, tmp_path: Path) -> None:
    """Test behavior when optional files (patent/exclusivity) are missing."""
    files = [
        tmp_path / "products.txt",
    ]
    for f in files:
        f.touch()

    mapping = fda_source.resolve_product_files(files)
    assert len(mapping["products"]) == 1
    assert len(mapping["patent"]) == 0
    assert len(mapping["exclusivity"]) == 0
