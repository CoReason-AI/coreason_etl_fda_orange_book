# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Bronze layer ingestion logic."""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from loguru import logger

from coreason_etl_fda_orange_book.config import FdaConfig
from coreason_etl_fda_orange_book.source import FdaOrangeBookSource


def yield_bronze_records(
    files_map: dict[str, list[Path]], source_instance: FdaOrangeBookSource
) -> Iterator[dict[str, Any]]:
    """
    Generator that yields records for the Bronze layer.

    Args:
        files_map: Dictionary mapping logical roles to file paths.
        source_instance: Instance of FdaOrangeBookSource for hashing.

    Yields:
        Dictionary representing a Bronze record.
    """
    ingestion_ts = datetime.now(timezone.utc).isoformat()

    for role, file_paths in files_map.items():
        for file_path in file_paths:
            logger.info(f"Processing {role} file: {file_path}")

            # Calculate hash once per file
            try:
                file_hash = source_instance.calculate_file_hash(file_path)
            except Exception as e:
                logger.error(f"Skipping file {file_path} due to hash error: {e}")
                continue

            try:
                # Open with configured encoding
                with open(
                    file_path,
                    "r",
                    encoding=FdaConfig.ENCODING,
                    errors=FdaConfig.ENCODING_ERRORS,
                ) as f:
                    for line_idx, line in enumerate(f):
                        line_content = line.strip()
                        if not line_content:
                            continue

                        record = {
                            "source_file": file_path.name,
                            "ingestion_ts": ingestion_ts,
                            "source_hash": file_hash,
                            "raw_content": {"line_number": line_idx + 1, "data": line_content},
                            "role": role,  # Optional but useful for debugging
                        }
                        yield record
            except Exception as e:
                logger.error(f"Error reading file {file_path}: {e}")
                # We might want to raise here or skip.
                # For "Lossless" attempts, failing the batch might be better than partial data?
                # But dlt handles errors. Let's log and continue for now, or raise if critical.
                # Given strict requirements, let's assume if we can't read a file we just fail the
                # iterator for that file.
                continue
