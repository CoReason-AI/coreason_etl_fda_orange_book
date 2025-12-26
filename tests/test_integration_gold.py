# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Integration tests for Gold layer ingestion."""

from pathlib import Path

import pytest

from coreason_etl_fda_orange_book.gold.ingestion import gold_products_resource


class TestGoldIntegration:
    """Integration tests for Gold layer DLT resource."""

    @pytest.fixture(name="mock_files")  # type: ignore
    def mock_files(self, tmp_path: Path) -> dict[str, list[Path]]:
        """Create mock files for testing."""
        # Products
        p_path = tmp_path / "products.txt"
        p_content = (
            "Ingredient~DF;Route~Trade_Name~Applicant~Strength~Appl_Type~Appl_No~Product_No~TE_Code~"
            "Approval_Date~RLD~Type~Applicant_Full_Name\n"
            "Budesonide~AEROSOL, FOAM;RECTAL~UCERIS~SALIX~2MG/ACTUATION~N~205613~001~~Oct 7, 2014~"
            "Yes~RX~SALIX PHARMACEUTICALS INC\n"
        )
        p_path.write_text(p_content, encoding="utf-8")

        # Patents
        pat_path = tmp_path / "patent.txt"
        pat_content = (
            "Appl_Type~Appl_No~Product_No~Patent_No~Patent_Expire_Date_Text~Drug_Substance_Flag~"
            "Drug_Product_Flag~Patent_Use_Code~Delist_Flag~Submission_Date\n"
            "N~205613~001~7654321~Jan 15, 2025~Y~N~U-123~N~Feb 1, 2010"
        )
        pat_path.write_text(pat_content, encoding="utf-8")

        # Exclusivity
        exc_path = tmp_path / "exclusivity.txt"
        exc_content = "Appl_Type~Appl_No~Product_No~Exclusivity_Code~Exclusivity_Date\nN~205613~001~ODE~Mar 10, 2026"
        exc_path.write_text(exc_content, encoding="utf-8")

        return {
            "products": [p_path],
            "patent": [pat_path],
            "exclusivity": [exc_path],
        }

    def test_gold_products_resource(self, mock_files: dict[str, list[Path]]) -> None:
        """Test Gold Products resource yields enriched data."""
        data = list(gold_products_resource(mock_files))
        assert len(data) == 1
        row = data[0]
        assert row["ingredient"] == "Budesonide"
        assert row["patent_number"] == "7654321"  # Joined successfully
        assert row["exclusivity_code"] == "ODE"  # Joined successfully
        assert row["search_vector_text"] == "UCERIS Budesonide"

    def test_gold_products_missing_files(self) -> None:
        """Test Gold resource with missing files."""
        # No products
        assert list(gold_products_resource({})) == []
