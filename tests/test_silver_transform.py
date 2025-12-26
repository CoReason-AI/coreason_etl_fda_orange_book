# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Tests for Silver layer transformations."""

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from coreason_etl_fda_orange_book.config import FdaConfig
from coreason_etl_fda_orange_book.silver.transform import (
    _generate_coreason_id,
    _parse_fda_date,
    transform_exclusivity,
    transform_patents,
    transform_products,
)


class TestSilverTransform:
    """Tests for transformation logic."""

    def test_generate_coreason_id(self):
        """Test UUID generation consistency."""
        sid = "001234001RX"
        uid1 = _generate_coreason_id(sid)
        uid2 = _generate_coreason_id(sid)
        assert uid1 == uid2
        assert len(uid1) == 36

    def test_parse_fda_date(self):
        """Test date parsing logic."""
        assert _parse_fda_date("Jan 1, 1982") == "1982-01-01"
        assert _parse_fda_date("Approved prior to Jan 1, 1982") is None
        assert _parse_fda_date(None) is None
        assert _parse_fda_date("") is None
        assert _parse_fda_date("Invalid") is None

    def test_transform_products_happy_path(self, tmp_path: Path):
        """Test standard transformation with a valid file."""
        content = (
            "Ingredient~DF;Route~Trade_Name~Applicant~Strength~Appl_Type~Appl_No~Product_No~TE_Code~Approval_Date~RLD~Type~Applicant_Full_Name\n"
            "Budesonide~AEROSOL, FOAM;RECTAL~UCERIS~SALIX~2MG/ACTUATION~N~205613~001~~Oct 7, 2014~Yes~RX~SALIX PHARMACEUTICALS INC\n"
        )
        f_path = tmp_path / "products.txt"
        f_path.write_text(content, encoding="utf-8")

        df = transform_products(f_path)

        assert not df.is_empty()
        row1 = df.row(0, named=True)
        assert row1["ingredient"] == "Budesonide"
        assert row1["application_number"] == "205613"
        assert row1["is_rld"] is True
        assert row1["marketing_status"] == "RX"

    def test_transform_products_padding(self, tmp_path: Path):
        """Test padding of Appl_No and Product_No."""
        content = (
            "Ingredient~Trade_Name~Applicant~Strength~Appl_No~Product_No~TE_Code~Approval_Date~RLD~Type\n"
            "DrugA~TradeA~AppA~10mg~123~1~~Jan 1, 2020~No~RX"
        )
        f_path = tmp_path / "padding.txt"
        f_path.write_text(content, encoding="utf-8")

        df = transform_products(f_path)
        row = df.row(0, named=True)
        assert row["application_number"] == "000123"
        assert row["product_number"] == "001"

    def test_transform_products_missing_type_column(self, tmp_path: Path):
        """Test fallback when 'Type' column is missing."""
        content = (
            "Ingredient~Trade_Name~Applicant~Strength~Appl_No~Product_No~TE_Code~Approval_Date~RLD\n"
            "DrugB~TradeB~AppB~20mg~456~2~~Feb 2, 2021~Yes"
        )
        f_path = tmp_path / "rx.txt"
        f_path.write_text(content, encoding="utf-8")

        df = transform_products(f_path, marketing_status_hint="DISCN")
        row = df.row(0, named=True)
        assert row["marketing_status"] == "DISCN"

    def test_transform_patents(self, tmp_path: Path):
        """Test patent transformation."""
        content = (
            "Appl_Type~Appl_No~Product_No~Patent_No~Patent_Expire_Date_Text~Drug_Substance_Flag~Drug_Product_Flag~Patent_Use_Code~Delist_Flag~Submission_Date\n"
            "N~020563~001~7654321~Jan 15, 2025~Y~N~U-123~N~Feb 1, 2010"
        )
        f_path = tmp_path / "patent.txt"
        f_path.write_text(content, encoding="utf-8")

        df = transform_patents(f_path)
        assert not df.is_empty()
        row = df.row(0, named=True)
        assert row["application_number"] == "020563"
        assert row["product_number"] == "001"
        assert row["patent_number"] == "7654321"
        assert row["patent_expiry_date"] == date(2025, 1, 15)
        assert row["is_drug_substance"] is True
        assert row["is_drug_product"] is False
        assert row["is_delisted"] is False
        assert row["submission_date"] == date(2010, 2, 1)

    def test_transform_exclusivity(self, tmp_path: Path):
        """Test exclusivity transformation."""
        content = (
            "Appl_Type~Appl_No~Product_No~Exclusivity_Code~Exclusivity_Date\n"
            "N~123456~002~ODE~Mar 10, 2026"
        )
        f_path = tmp_path / "exclusivity.txt"
        f_path.write_text(content, encoding="utf-8")

        df = transform_exclusivity(f_path)
        assert not df.is_empty()
        row = df.row(0, named=True)
        assert row["application_number"] == "123456"
        assert row["product_number"] == "002"
        assert row["exclusivity_code"] == "ODE"
        assert row["exclusivity_end_date"] == date(2026, 3, 10)

    def test_empty_and_bad_files(self, tmp_path: Path):
        """Test handling of empty and bad files for all transforms."""
        f_path = tmp_path / "empty.txt"
        f_path.write_text("", encoding="utf-8")
        assert transform_products(f_path).is_empty()
        assert transform_patents(f_path).is_empty()
        assert transform_exclusivity(f_path).is_empty()

        f_path = tmp_path / "bad.txt"
        f_path.write_text("Not~Valid~CSV", encoding="utf-8")
        # Should execute safely
        transform_products(f_path)
