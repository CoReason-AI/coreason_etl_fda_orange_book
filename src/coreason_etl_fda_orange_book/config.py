"""Configuration module for FDA Orange Book ETL."""
from typing import Final


class FdaConfig:
    """Configuration constants for the FDA Orange Book pipeline."""

    # Source Definition
    DEFAULT_BASE_URL: Final[str] = "https://www.fda.gov/media/76860/download?attachment"

    # File Names
    FILE_PRODUCTS: Final[str] = "products.txt"
    FILE_PATENTS: Final[str] = "patent.txt"
    FILE_EXCLUSIVITY: Final[str] = "exclusivity.txt"

    # Parsing
    DELIMITER: Final[str] = "~"
    ENCODING: Final[str] = "utf8-lossy"  # Handling for potential encoding issues

    # Identity Resolution
    NAMESPACE_FDA: Final[str] = "fda.gov"  # For UUID5 generation
