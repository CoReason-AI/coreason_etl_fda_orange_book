"""Tests for the configuration module."""
from coreason_etl_fda_orange_book.config import FdaConfig


def test_fda_config_defaults() -> None:
    """Test that default configuration values are set correctly."""
    assert FdaConfig.DEFAULT_BASE_URL == "https://www.fda.gov/media/76860/download?attachment"
    assert FdaConfig.FILE_PRODUCTS == "products.txt"
    assert FdaConfig.FILE_PATENTS == "patent.txt"
    assert FdaConfig.FILE_EXCLUSIVITY == "exclusivity.txt"
    assert FdaConfig.DELIMITER == "~"
    assert FdaConfig.ENCODING == "utf8-lossy"
    assert FdaConfig.NAMESPACE_FDA == "fda.gov"
