# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Complex integration scenarios and edge case tests."""

from pathlib import Path

import polars as pl

from coreason_etl_fda_orange_book.gold.logic import create_gold_view
from coreason_etl_fda_orange_book.silver.transform import (
    transform_exclusivity,
    transform_patents,
    transform_products,
)


class TestComplexScenarios:
    """Tests covering multi-step flows and complex data relationships."""

    def test_end_to_end_logic_flow(self, tmp_path: Path) -> None:
        """
        Verify the full logic flow from raw text to Gold view.

        Scenario:
        - Product A: Active (RX), has Patent P1, Exclusivity E1.
        - Product B: Active (OTC), has Patent P2, No Exclusivity.
        - Product C: Active (RX), No Patent, Exclusivity E2.
        - Product D: Discontinued (DISCN), has Patent P3.
        """
        # 1. Setup Files
        prod_file = tmp_path / "products.txt"
        prod_file.write_text(
            "Ingredient~Trade_Name~Applicant~Strength~Appl_No~Product_No~TE_Code~Approval_Date~RLD~Type\n"
            "IngA~TradeA~AppA~10mg~000001~001~~Jan 1, 2020~Yes~RX\n"
            "IngB~TradeB~AppB~10mg~000002~001~~Jan 1, 2020~No~OTC\n"
            "IngC~TradeC~AppC~10mg~000003~001~~Jan 1, 2020~No~RX\n"
            "IngD~TradeD~AppD~10mg~000004~001~~Jan 1, 2020~No~DISCN\n",
            encoding="utf-8",
        )

        pat_file = tmp_path / "patent.txt"
        pat_file.write_text(
            "Appl_No~Product_No~Patent_No~Patent_Expire_Date_Text~Drug_Substance_Flag~Drug_Product_Flag~Patent_Use_Code~Delist_Flag~Submission_Date\n"
            "000001~001~P1~Jan 1, 2030~Y~N~U1~N~Jan 1, 2010\n"
            "000002~001~P2~Jan 1, 2030~N~Y~U2~N~Jan 1, 2010\n"
            "000004~001~P3~Jan 1, 2030~N~N~U3~N~Jan 1, 2010\n",
            encoding="utf-8",
        )

        exc_file = tmp_path / "exclusivity.txt"
        exc_file.write_text(
            "Appl_No~Product_No~Exclusivity_Code~Exclusivity_Date\n"
            "000001~001~E1~Jan 1, 2025\n"
            "000003~001~E2~Jan 1, 2025\n",
            encoding="utf-8",
        )

        # 2. Transform to Silver
        df_prod = transform_products(prod_file)
        df_pat = transform_patents(pat_file)
        df_exc = transform_exclusivity(exc_file)

        assert df_prod.height == 4
        assert df_pat.height == 3
        assert df_exc.height == 2

        # 3. Create Gold View (Default: Exclude Discontinued)
        df_gold = create_gold_view(df_prod, df_pat, df_exc)

        # Should contain A, B, C (3 rows). D is DISCN.
        assert df_gold.height == 3

        # Verify Product A (Full Join)
        row_a = df_gold.filter(pl.col("application_number") == "000001").row(0, named=True)
        assert row_a["patent_number"] == "P1"
        assert row_a["exclusivity_code"] == "E1"

        # Verify Product B (Patent only)
        row_b = df_gold.filter(pl.col("application_number") == "000002").row(0, named=True)
        assert row_b["patent_number"] == "P2"
        assert row_b["exclusivity_code"] is None

        # Verify Product C (Exclusivity only)
        row_c = df_gold.filter(pl.col("application_number") == "000003").row(0, named=True)
        assert row_c["patent_number"] is None
        assert row_c["exclusivity_code"] == "E2"

        # 4. Create Gold View (Include Discontinued)
        df_gold_all = create_gold_view(df_prod, df_pat, df_exc, include_discontinued=True)
        assert df_gold_all.height == 4

        # Verify Product D
        row_d = df_gold_all.filter(pl.col("application_number") == "000004").row(0, named=True)
        assert row_d["marketing_status"] == "DISCN"
        assert row_d["patent_number"] == "P3"

    def test_one_to_many_patent_explosion(self, tmp_path: Path) -> None:
        """
        Verify that a single product with multiple patents results in multiple rows.
        """
        prod_file = tmp_path / "products.txt"
        prod_file.write_text(
            "Ingredient~Trade_Name~Applicant~Strength~Appl_No~Product_No~TE_Code~Approval_Date~RLD~Type\n"
            "IngA~TradeA~AppA~10mg~000001~001~~Jan 1, 2020~Yes~RX\n",
            encoding="utf-8",
        )

        pat_file = tmp_path / "patent.txt"
        # Two patents for the same Appl_No/Product_No
        pat_file.write_text(
            "Appl_No~Product_No~Patent_No~Patent_Expire_Date_Text~Drug_Substance_Flag~Drug_Product_Flag~Patent_Use_Code~Delist_Flag~Submission_Date\n"
            "000001~001~P1~Jan 1, 2030~Y~N~U1~N~Jan 1, 2010\n"
            "000001~001~P2~Jan 1, 2032~N~Y~U2~N~Jan 1, 2012\n",
            encoding="utf-8",
        )

        # No Exclusivity
        exc_file = tmp_path / "exclusivity.txt"
        exc_file.write_text("Appl_No~Product_No~Exclusivity_Code~Exclusivity_Date\n", encoding="utf-8")

        df_prod = transform_products(prod_file)
        df_pat = transform_patents(pat_file)
        df_exc = transform_exclusivity(exc_file)

        df_gold = create_gold_view(df_prod, df_pat, df_exc)

        # Should explode to 2 rows
        assert df_gold.height == 2

        patents = df_gold["patent_number"].to_list()
        assert "P1" in patents
        assert "P2" in patents

        # Verify base product data is duplicated correctly
        assert df_gold["trade_name"].to_list() == ["TradeA", "TradeA"]

    def test_dirty_data_handling(self, tmp_path: Path) -> None:
        """
        Verify robust handling of messy input data.
        - Whitespace in keys.
        - Mixed case headers.
        - Inconsistent boolean flags.
        """
        prod_file = tmp_path / "products.txt"
        # Note: " Appl_No " (spaces), "YES"/"no" (case), " Product_No " (spaces in header)
        # We need to verify if our logic strips whitespace from VALUES too.
        # transform_products logic: `df = df.rename({col: col.strip() for col in df.columns})` handles header spaces.
        # `df = df.with_columns(pl.all().cast(pl.String).str.strip_chars())` handles value spaces.

        prod_file.write_text(
            "Ingredient~ Trade_Name ~Applicant~Strength~ Appl_No ~ Product_No ~TE_Code~Approval_Date~RLD~Type\n"
            "IngA~TradeA~AppA~10mg~ 000001 ~ 001 ~~Jan 1, 2020~ YES ~RX\n",
            encoding="utf-8",
        )

        df_prod = transform_products(prod_file)

        assert df_prod.height == 1
        row = df_prod.row(0, named=True)

        # Verify whitespace stripping on keys
        assert row["application_number"] == "000001"
        assert row["product_number"] == "001"

        # Verify Boolean parsing (YES -> True)
        assert row["is_rld"] is True

        # Verify header cleaning (Trade_Name was accessible despite spaces in file)
        assert row["trade_name"] == "TradeA"

        # Check source_id generation uses cleaned values
        # "000001" + "001" + "RX" -> source_id
        assert row["source_id"] == "000001001RX"
