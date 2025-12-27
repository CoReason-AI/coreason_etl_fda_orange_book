# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Complex logic tests for Gold layer."""

import polars as pl

from coreason_etl_fda_orange_book.gold.logic import create_gold_view


class TestGoldLogicComplex:
    """Tests covering complex logic and edge cases in Gold layer."""

    def test_many_to_many_join(self) -> None:
        """
        Verify Cartesian product behavior when a product has multiple patents AND multiple exclusivity codes.
        Expectation: Rows = 1 product * N patents * M exclusivities.
        """
        # Product A
        products_df = pl.DataFrame(
            {
                "application_number": ["000001"],
                "product_number": ["001"],
                "trade_name": ["DrugA"],
                "ingredient": ["IngA"],
                "marketing_status": ["RX"],
            }
        )

        # 2 Patents for Product A
        patents_df = pl.DataFrame(
            {
                "application_number": ["000001", "000001"],
                "product_number": ["001", "001"],
                "patent_number": ["P1", "P2"],
                "patent_expiry_date": ["2030-01-01", "2032-01-01"],
            }
        )

        # 3 Exclusivity codes for Product A
        exclusivity_df = pl.DataFrame(
            {
                "application_number": ["000001", "000001", "000001"],
                "product_number": ["001", "001", "001"],
                "exclusivity_code": ["E1", "E2", "E3"],
                "exclusivity_end_date": ["2025-01-01", "2026-01-01", "2027-01-01"],
            }
        )

        df_gold = create_gold_view(products_df, patents_df, exclusivity_df)

        # Expect 1 * 2 * 3 = 6 rows
        assert df_gold.height == 6

        # Verify all combinations exist
        combinations = df_gold.select(["patent_number", "exclusivity_code"]).unique()
        assert combinations.height == 6

        pats = df_gold["patent_number"].unique().to_list()
        excs = df_gold["exclusivity_code"].unique().to_list()
        pats.sort()
        excs.sort()

        assert pats == ["P1", "P2"]
        assert excs == ["E1", "E2", "E3"]

    def test_empty_auxiliary_tables(self) -> None:
        """
        Verify schema and data when patents and exclusivity tables are empty.
        Expectation: Left join preserves products, adds null columns for missing tables.
        """
        products_df = pl.DataFrame(
            {
                "application_number": ["000001"],
                "product_number": ["001"],
                "trade_name": ["DrugA"],
                "ingredient": ["IngA"],
                "marketing_status": ["RX"],
            }
        )

        patents_df = pl.DataFrame()
        exclusivity_df = pl.DataFrame()

        df_gold = create_gold_view(products_df, patents_df, exclusivity_df)

        assert df_gold.height == 1

        # Check specific columns added by logic for empty tables
        assert "patent_number" in df_gold.columns
        assert "exclusivity_code" in df_gold.columns

        # Verify values are null
        assert df_gold["patent_number"][0] is None
        assert df_gold["exclusivity_code"][0] is None

    def test_empty_products_table(self) -> None:
        """
        Verify behavior when products table is empty.
        Expectation: Return empty DataFrame immediately.
        """
        products_df = pl.DataFrame(
            schema={
                "application_number": pl.String,
                "product_number": pl.String,
                "trade_name": pl.String,
                "ingredient": pl.String,
                "marketing_status": pl.String,
            }
        )

        patents_df = pl.DataFrame(
            {"application_number": ["000001"], "product_number": ["001"], "patent_number": ["P1"]}
        )

        exclusivity_df = pl.DataFrame()

        df_gold = create_gold_view(products_df, patents_df, exclusivity_df)

        assert df_gold.is_empty()

    def test_discontinued_filtering_edge_cases(self) -> None:
        """
        Verify filtering of 'DISCN' status with inconsistent casing or whitespace.
        Expectation: Current logic only filters exact 'DISCN'.
        This test documents current behavior or reveals if normalization is needed.
        """
        products_df = pl.DataFrame(
            {
                "application_number": ["001", "002", "003", "004"],
                "product_number": ["001", "001", "001", "001"],
                "trade_name": ["A", "B", "C", "D"],
                "ingredient": ["I", "I", "I", "I"],
                "marketing_status": ["DISCN", "discn", " DISCN ", "RX"],
            }
        )

        patents_df = pl.DataFrame()
        exclusivity_df = pl.DataFrame()

        # Default: exclude discontinued
        df_gold = create_gold_view(products_df, patents_df, exclusivity_df, include_discontinued=False)

        # Logic checks != "DISCN"
        # "DISCN" -> Filtered
        # "discn" -> Kept (unless logic changes to normalize)
        # " DISCN " -> Kept (unless logic changes to normalize)
        # "RX" -> Kept

        statuses = df_gold["marketing_status"].to_list()

        assert "DISCN" not in statuses
        assert "RX" in statuses

        # Checking edge cases. If logic is strict "!= DISCN", these will be present.
        # If the intention of the test is to ensure they are filtered, we would need to update the code.
        # For now, we test the logic as is, or updated logic if we decide to fix it.
        # Given "test correctness of logic", if "discn" means discontinued, it SHOULD probably be filtered.
        # But let's assert current behavior first or decide if this constitutes a bug fix.
        # The prompt asked to "add tests".
        # I will assert that they are PRESENT currently, confirming the strictness of the filter.
        assert "discn" in statuses
        assert " DISCN " in statuses

    def test_join_on_null_keys(self) -> None:
        """
        Verify join behavior when keys are Null.
        Expectation: Rows with null keys in Products should not match rows with null keys in Patents/Exclusivity
        (standard SQL behavior), or behave as per Polars default (null matches null? No, usually null != null).
        """
        products_df = pl.DataFrame(
            {
                "application_number": [None],
                "product_number": ["001"],
                "trade_name": ["DrugNull"],
                "ingredient": ["IngNull"],
                "marketing_status": ["RX"],
            }
        )

        patents_df = pl.DataFrame(
            {
                "application_number": [None],
                "product_number": ["001"],
                "patent_number": ["P1"],
            }
        )

        exclusivity_df = pl.DataFrame()

        df_gold = create_gold_view(products_df, patents_df, exclusivity_df)

        # Polars join behavior on nulls:
        # By default, nulls do NOT match nulls in join keys.
        assert df_gold.height == 1
        assert df_gold["patent_number"][0] is None

    def test_schema_consistency_extra_columns(self) -> None:
        """
        Verify that extra columns in input DataFrames are preserved in the output.
        """
        products_df = pl.DataFrame(
            {
                "application_number": ["000001"],
                "product_number": ["001"],
                "trade_name": ["DrugA"],
                "ingredient": ["IngA"],
                "marketing_status": ["RX"],
                "extra_prod_col": ["KeepMe"],
            }
        )

        patents_df = pl.DataFrame(
            {
                "application_number": ["000001"],
                "product_number": ["001"],
                "patent_number": ["P1"],
                "extra_pat_col": ["KeepMePat"],
            }
        )

        exclusivity_df = pl.DataFrame()

        df_gold = create_gold_view(products_df, patents_df, exclusivity_df)

        assert "extra_prod_col" in df_gold.columns
        assert "extra_pat_col" in df_gold.columns
        assert df_gold["extra_prod_col"][0] == "KeepMe"
        assert df_gold["extra_pat_col"][0] == "KeepMePat"
