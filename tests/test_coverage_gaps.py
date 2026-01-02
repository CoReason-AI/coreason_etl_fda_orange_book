# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Additional integration tests to cover missing branches."""

from pathlib import Path

from coreason_etl_fda_orange_book.gold.ingestion import gold_products_resource
from coreason_etl_fda_orange_book.silver.ingestion import (
    silver_exclusivity_resource,
    silver_patents_resource,
    silver_products_resource,
)


class TestCoverageGaps:
    """Tests specifically targeting missed branches."""

    def test_silver_products_missing_key(self) -> None:
        """Test missing 'products' key in files_map."""
        data = list(silver_products_resource({}))
        assert data == []

    def test_silver_patents_missing_key(self) -> None:
        """Test missing 'patent' key in files_map."""
        data = list(silver_patents_resource({}))
        assert data == []

    def test_silver_exclusivity_missing_key(self) -> None:
        """Test missing 'exclusivity' key in files_map."""
        data = list(silver_exclusivity_resource({}))
        assert data == []

    def test_gold_products_missing_key(self) -> None:
        """Test missing 'products' key in files_map."""
        data = list(gold_products_resource({}))
        assert data == []

    def test_gold_products_no_valid_product_dfs(self, tmp_path: Path) -> None:
        """Test when product files exist but yield empty dataframes."""
        # Create empty product file
        p_path = tmp_path / "products.txt"
        p_path.write_text("", encoding="utf-8")

        files_map = {"products": [p_path]}

        # Should return early
        data = list(gold_products_resource(files_map))
        assert data == []

    def test_silver_ingestion_hints(self, tmp_path: Path) -> None:
        """Test different filename hints in ingestion."""
        # Setup files with specific names
        otc = tmp_path / "otc.txt"
        otc.write_text(
            (
                "Ingredient~Trade_Name~Applicant~Strength~Appl_No~Product_No~TE_Code~Approval_Date~RLD\n"
                "I~T~A~S~001~001~~Jan 1, 2020~No"
            ),
            encoding="utf-8",
        )

        disc = tmp_path / "disc.txt"
        disc.write_text(
            (
                "Ingredient~Trade_Name~Applicant~Strength~Appl_No~Product_No~TE_Code~Approval_Date~RLD\n"
                "I~T~A~S~001~001~~Jan 1, 2020~No"
            ),
            encoding="utf-8",
        )

        files_map = {"products": [otc, disc]}

        # Just run to hit the branches
        data = list(silver_products_resource(files_map))
        # DLT resource yields dicts (when iterating directly on resource)
        statuses = {row["marketing_status"] for row in data}
        assert "OTC" in statuses
        assert "DISCN" in statuses

    def test_gold_ingestion_hints_and_concats(self, tmp_path: Path) -> None:
        """Test gold ingestion aggregation branches."""
        # Setup valid product files
        p1 = tmp_path / "rx.txt"
        p1.write_text(
            (
                "Ingredient~Trade_Name~Applicant~Strength~Appl_No~Product_No~TE_Code~Approval_Date~RLD\n"
                "I~T~A~S~001~001~~Jan 1, 2020~No"
            ),
            encoding="utf-8",
        )

        # Setup valid patent file
        pat = tmp_path / "patent.txt"
        pat.write_text(
            (
                "Appl_No~Product_No~Patent_No~Patent_Expire_Date_Text~Drug_Substance_Flag~Drug_Product_Flag~"
                "Patent_Use_Code~Delist_Flag\n001~001~123~Jan 1, 2030~Y~N~U~N"
            ),
            encoding="utf-8",
        )

        # Setup valid exclusivity file
        exc = tmp_path / "exclusivity.txt"
        exc.write_text("Appl_No~Product_No~Exclusivity_Code~Exclusivity_Date\n001~001~E~Jan 1, 2025", encoding="utf-8")

        # Add OTC and DISC to hit branches in gold_products_resource loop
        otc = tmp_path / "otc.txt"
        otc.write_text(
            (
                "Ingredient~Trade_Name~Applicant~Strength~Appl_No~Product_No~TE_Code~Approval_Date~RLD\n"
                "I2~T2~A~S~002~001~~Jan 1, 2020~No"
            ),
            encoding="utf-8",
        )

        disc = tmp_path / "disc.txt"
        disc.write_text(
            (
                "Ingredient~Trade_Name~Applicant~Strength~Appl_No~Product_No~TE_Code~Approval_Date~RLD\n"
                "I3~T3~A~S~003~001~~Jan 1, 2020~No"
            ),
            encoding="utf-8",
        )

        files_map = {"products": [p1, otc, disc], "patent": [pat], "exclusivity": [exc]}

        data = list(gold_products_resource(files_map))
        assert len(data) > 0

    def test_silver_empty_dataframe_continue(self, tmp_path: Path) -> None:
        """Test continue on empty DF in silver resources."""
        empty = tmp_path / "empty.txt"
        empty.write_text("", encoding="utf-8")

        files_map = {"products": [empty], "patent": [empty], "exclusivity": [empty]}

        assert list(silver_products_resource(files_map)) == []
        assert list(silver_patents_resource(files_map)) == []
        assert list(silver_exclusivity_resource(files_map)) == []
