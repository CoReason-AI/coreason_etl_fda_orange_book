# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Ingestion logic for the Bronze layer."""

from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import dlt
from loguru import logger

from coreason_etl_fda_orange_book.config import FdaConfig
from coreason_etl_fda_orange_book.source import FdaOrangeBookSource


def yield_bronze_records(
    files_map: dict[str, list[Path]],
    source: FdaOrangeBookSource,
) -> Iterator[dict[str, Any]]:
    """
    Generator that reads files and yields raw records for the Bronze layer.

    Args:
        files_map: Dictionary mapping roles (e.g., 'products') to lists of file paths.
        source: Instance of FdaOrangeBookSource for hashing and utility.

    Yields:
        Dictionary containing metadata and raw content for each line.
    """
    ingestion_ts = datetime.now(timezone.utc).isoformat()

    for role, file_paths in files_map.items():
        for file_path in file_paths:
            logger.info(f"Processing {file_path} for role {role}")

            try:
                # Calculate hash
                source_hash = source.calculate_file_hash(file_path)
            except Exception as e:
                logger.error(f"Failed to calculate hash for {file_path}: {e}")
                continue

            try:
                with open(file_path, "r", encoding=FdaConfig.ENCODING, errors=FdaConfig.ENCODING_ERRORS) as f:
                    for line_idx, line in enumerate(f, start=1):
                        line_content = line.strip()
                        if not line_content:
                            continue

                        yield {
                            "source_file": file_path.name,
                            "ingestion_ts": ingestion_ts,
                            "source_hash": source_hash,
                            "raw_content": {
                                "line_number": line_idx,
                                "data": line_content,
                            },
                            "role": role,
                        }
            except Exception as e:
                logger.error(f"Failed to read file {file_path}: {e}")
                continue


@dlt.resource(name="bronze_fda_orange_book", write_disposition="append")
def bronze_resource(
    files_map: dict[str, list[Path]],
    source: FdaOrangeBookSource,
) -> Iterator[dict[str, Any]]:
    """
    DLT resource wrapper for the Bronze layer ingestion.

    Args:
        files_map: Dictionary mapping roles to file paths.
        source: FdaOrangeBookSource instance.

    Returns:
        Iterator of bronze records.
    """
    yield from yield_bronze_records(files_map, source)
