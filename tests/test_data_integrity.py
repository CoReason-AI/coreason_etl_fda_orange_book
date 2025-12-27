# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Data integrity and edge case tests for Silver transformations."""

from datetime import date
from pathlib import Path

import polars as pl

from coreason_etl_fda_orange_book.silver.transform import (
    transform_products,
)


class TestDataIntegrity:
    """Tests focusing on data integrity: bad dates, missing fields, duplicates, and schema variations."""

    def test_malformed_dates(self, tmp_path: Path) -> None:
        """
        Test that `transform_products` correctly parses valid dates and handles invalid ones.
        """
        f = tmp_path / "dates.txt"
        # 1. Valid: Jan 1, 2020
        # 2. Invalid Format: 2020-01-01 (should fail _parse_fda_date expects 'Jan 1, 2020')
        # 3. Approved prior: Approved prior to Jan 1, 1982
        # 4. Garbage: Not a date
        # 5. Empty: ~
        content = (
            "Ingredient~Trade_Name~Applicant~Strength~Appl_No~Product_No~TE_Code~Approval_Date~RLD~Type\n"
            "I1~T1~A1~S1~001~001~AB~Jan 1, 2020~No~RX\n"
            "I2~T2~A2~S2~002~001~AB~2020-01-01~No~RX\n"
            "I3~T3~A3~S3~003~001~AB~Approved prior to Jan 1, 1982~No~RX\n"
            "I4~T4~A4~S4~004~001~AB~GarbageString~No~RX\n"
            "I5~T5~A5~S5~005~001~AB~~No~RX\n"
        )
        f.write_text(content, encoding="utf-8")

        df = transform_products(f)

        # Sort by appl_no to guarantee order
        df = df.sort("application_number")
        dates = df["approval_date"].to_list()

        # 1. Valid -> 2020-01-01
        assert dates[0] == date(2020, 1, 1)

        # 2. Invalid Format -> None (because logic expects specific format)
        assert dates[1] is None

        # 3. Approved prior -> None
        assert dates[2] is None

        # 4. Garbage -> None
        assert dates[3] is None

        # 5. Empty -> None
        assert dates[4] is None

    def test_missing_required_fields(self, tmp_path: Path) -> None:
        """
        Verify that rows with missing keys (Appl_No, Product_No) are dropped,
        but rows with missing optional fields are kept.
        """
        f = tmp_path / "missing_fields.txt"
        # Row 1: Valid
        # Row 2: Missing Appl_No
        # Row 3: Missing Product_No
        # Row 4: Missing Strength (Optional)
        content = (
            "Ingredient~Trade_Name~Applicant~Strength~Appl_No~Product_No~TE_Code~Approval_Date~RLD~Type\n"
            "I1~T1~A1~S1~000001~001~AB~Jan 1, 2020~No~RX\n"
            "I2~T2~A2~S2~~001~AB~Jan 1, 2020~No~RX\n"
            "I3~T3~A3~S3~000003~~AB~Jan 1, 2020~No~RX\n"
            "I4~T4~A4~~000004~001~AB~Jan 1, 2020~No~RX\n"
        )
        f.write_text(content, encoding="utf-8")

        df = transform_products(f)

        # Should have 2 rows (Row 1 and Row 4)
        # Note: transform_products filters where source_id is not null.
        # source_id = Appl + Prod + Status.
        # If Appl or Prod is missing/empty, resulting source_id might be weird or empty string?
        # Let's check logic:
        # safe_col_str("Appl_No") -> if missing, returns None cast to String ("null"?) or null?
        # Actually pl.lit(None).cast(pl.String) is null.
        # null + string -> null in Polars.
        # So source_id becomes null.
        # Then filter(pl.col("source_id").is_not_null()) removes it.

        assert df.height == 2
        appl_nums = df["application_number"].sort().to_list()
        assert appl_nums == ["000001", "000004"]

        # Check Row 4 has null strength
        row4 = df.filter(pl.col("application_number") == "000004").row(0, named=True)
        # Depending on CSV reading, empty field might be "" or null.
        # _clean_read_csv uses default which might interpret empty as null or ""?
        # transform_products does `str.strip_chars()`.
        # If it was empty string "", stripped is "".
        # If it was null, it remains null.
        # Let's just check it is falsy (None or "")
        assert not row4["strength"]

    def test_duplicate_records(self, tmp_path: Path) -> None:
        """
        Verify behavior with duplicate records.
        Current expectation: Duplicates are preserved in Silver.
        (Gold might handle them or they fan out, but Silver just transforms what is there).
        """
        f = tmp_path / "duplicates.txt"
        # Duplicate rows
        content = (
            "Ingredient~Trade_Name~Applicant~Strength~Appl_No~Product_No~TE_Code~Approval_Date~RLD~Type\n"
            "I1~T1~A1~S1~001~001~AB~Jan 1, 2020~No~RX\n"
            "I1~T1~A1~S1~001~001~AB~Jan 1, 2020~No~RX\n"
        )
        f.write_text(content, encoding="utf-8")

        df = transform_products(f)
        assert df.height == 2
        assert df["application_number"].to_list() == ["000001", "000001"]

    def test_application_number_padding(self, tmp_path: Path) -> None:
        """
        Verify Appl_No and Product_No are correctly padded.
        """
        f = tmp_path / "padding.txt"
        # Appl_No "123", Product_No "1"
        content = (
            "Ingredient~Trade_Name~Applicant~Strength~Appl_No~Product_No~TE_Code~Approval_Date~RLD~Type\n"
            "I1~T1~A1~S1~123~1~AB~Jan 1, 2020~No~RX\n"
        )
        f.write_text(content, encoding="utf-8")

        df = transform_products(f)
        row = df.row(0, named=True)
        assert row["application_number"] == "000123"
        assert row["product_number"] == "001"

    def test_extra_and_missing_columns(self, tmp_path: Path) -> None:
        """
        Test resilience to schema drift (extra cols, missing optional cols).
        """
        # 1. Extra Column
        f1 = tmp_path / "extra_col.txt"
        content1 = (
            "Ingredient~Trade_Name~Applicant~Strength~Appl_No~Product_No~TE_Code~Approval_Date~RLD~Type~ExtraCol\n"
            "I1~T1~A1~S1~001~001~AB~Jan 1, 2020~No~RX~ExtraValue\n"
        )
        f1.write_text(content1, encoding="utf-8")
        df1 = transform_products(f1)
        assert df1.height == 1
        # Extra col should simply be ignored as it's not selected

        # 2. Missing Optional Column (TE_Code)
        f2 = tmp_path / "missing_col.txt"
        content2 = (
            "Ingredient~Trade_Name~Applicant~Strength~Appl_No~Product_No~Approval_Date~RLD~Type\n"
            "I1~T1~A1~S1~001~001~Jan 1, 2020~No~RX\n"
        )
        f2.write_text(content2, encoding="utf-8")
        df2 = transform_products(f2)
        assert df2.height == 1
        # TE_Code should be null
        assert df2.row(0, named=True)["te_code"] is None
