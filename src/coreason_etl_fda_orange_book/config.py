# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Configuration module for FDA Orange Book ETL."""

from pathlib import Path
from typing import Final


class FdaConfig:
    """Configuration constants for the FDA Orange Book pipeline."""

    # Source Definition
    DEFAULT_BASE_URL: Final[str] = "https://www.fda.gov/media/76860/download?attachment"
    DEFAULT_DOWNLOAD_DIR: Final[Path] = Path("data/bronze")

    # File Names
    FILE_PRODUCTS: Final[str] = "products.txt"
    FILE_PATENTS: Final[str] = "patent.txt"
    FILE_EXCLUSIVITY: Final[str] = "exclusivity.txt"

    # Parsing
    DELIMITER: Final[str] = "~"
    ENCODING: Final[str] = "utf-8"  # Standard encoding
    ENCODING_ERRORS: Final[str] = "replace"  # Handling for potential encoding issues (lossy)

    # Identity Resolution
    NAMESPACE_FDA: Final[str] = "fda.gov"  # For UUID5 generation
