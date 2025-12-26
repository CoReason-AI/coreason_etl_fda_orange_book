# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Tests for strict Pydantic validation in Silver models."""

import pytest
from pydantic import ValidationError

from coreason_etl_fda_orange_book.silver.models import (
    SilverExclusivity,
    SilverPatent,
    SilverProduct,
)


class TestStrictValidation:
    """Tests ensuring models strictly reject incorrect types."""

    def test_silver_product_strict_strings(self) -> None:
        """Test strict string enforcement for SilverProduct."""
        valid_data = {
            "coreason_id": "uuid",
            "source_id": "src",
            "ingredient": "ing",
            "trade_name": "trade",
            "applicant_short": "app",
            "strength": "10mg",
            "application_number": "123456",  # valid string
            "product_number": "001",  # valid string
            "is_rld": True,
            "marketing_status": "RX",
        }
        # Should pass
        assert SilverProduct(**valid_data)

        # Should fail with integer application_number
        invalid_data = valid_data.copy()
        invalid_data["application_number"] = 123456  # int
        with pytest.raises(ValidationError) as excinfo:
            SilverProduct(**invalid_data)
        assert "Input should be a valid string" in str(excinfo.value)

        # Should fail with integer product_number
        invalid_data = valid_data.copy()
        invalid_data["product_number"] = 1  # int
        with pytest.raises(ValidationError) as excinfo:
            SilverProduct(**invalid_data)
        assert "Input should be a valid string" in str(excinfo.value)

    def test_silver_patent_strict_strings(self) -> None:
        """Test strict string enforcement for SilverPatent."""
        valid_data = {
            "application_number": "123456",
            "product_number": "001",
            "patent_number": "PAT123",
            "is_drug_substance": False,
            "is_drug_product": False,
            "is_delisted": False,
        }
        assert SilverPatent(**valid_data)

        # Fail on int application_number
        invalid_data = valid_data.copy()
        invalid_data["application_number"] = 123456
        with pytest.raises(ValidationError):
            SilverPatent(**invalid_data)

    def test_silver_exclusivity_strict_strings(self) -> None:
        """Test strict string enforcement for SilverExclusivity."""
        valid_data = {
            "application_number": "123456",
            "product_number": "001",
            "exclusivity_code": "ODE",
        }
        assert SilverExclusivity(**valid_data)

        # Fail on int product_number
        invalid_data = valid_data.copy()
        invalid_data["product_number"] = 1
        with pytest.raises(ValidationError):
            SilverExclusivity(**invalid_data)
