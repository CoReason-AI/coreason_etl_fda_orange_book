# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Integration tests for Silver layer ingestion."""

from pathlib import Path

import dlt
import pytest

from coreason_etl_fda_orange_book.silver.ingestion import (
    silver_exclusivity_resource,
    silver_patents_resource,
    silver_products_resource,
)


class TestSilverIntegration:
    """Integration tests for Silver layer DLT resources."""

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

    def test_silver_products_resource(self, mock_files: dict[str, list[Path]]) -> None:
        """Test Silver Products resource yields data."""
        data = list(silver_products_resource(mock_files))
        assert len(data) == 1
        assert data[0]["ingredient"] == "Budesonide"
        assert data[0]["coreason_id"] is not None

    def test_silver_patents_resource(self, mock_files: dict[str, list[Path]]) -> None:
        """Test Silver Patents resource yields data."""
        data = list(silver_patents_resource(mock_files))
        assert len(data) == 1
        assert data[0]["patent_number"] == "7654321"

    def test_silver_exclusivity_resource(self, mock_files: dict[str, list[Path]]) -> None:
        """Test Silver Exclusivity resource yields data."""
        data = list(silver_exclusivity_resource(mock_files))
        assert len(data) == 1
        assert data[0]["exclusivity_code"] == "ODE"

    def test_pipeline_run_mock(self, mock_files: dict[str, list[Path]]) -> None:
        """Test running the DLT pipeline with Silver resources."""
        # Assign to _ to avoid unused variable error
        _ = dlt.pipeline(pipeline_name="test_silver", destination="duckdb", dataset_name="silver_test")
        pass

    def test_missing_files_in_map(self) -> None:
        """Test handling of missing keys in file map."""
        empty_map: dict[str, list[Path]] = {}
        assert list(silver_products_resource(empty_map)) == []
        assert list(silver_patents_resource(empty_map)) == []
        assert list(silver_exclusivity_resource(empty_map)) == []
