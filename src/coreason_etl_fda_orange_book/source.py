# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_fda_orange_book

"""Source module for downloading and handling FDA Orange Book files."""

import hashlib
import shutil
import zipfile
from pathlib import Path
from typing import Final

import requests
from loguru import logger

from coreason_etl_fda_orange_book.config import FdaConfig
from coreason_etl_fda_orange_book.exceptions import SourceConnectionError, SourceSchemaError


class FdaOrangeBookSource:
    """Manages downloading and extracting FDA Orange Book data."""

    CHUNK_SIZE: Final[int] = 8192

    def __init__(self, base_url: str = FdaConfig.DEFAULT_BASE_URL) -> None:
        """
        Initialize the source with the FDA base URL.

        Args:
            base_url: The URL to download the ZIP from. Defaults to FdaConfig.DEFAULT_BASE_URL.
        """
        self.base_url = base_url

    def download_archive(self, destination: Path) -> None:
        """
        Download the FDA Orange Book ZIP archive to a local path.

        Args:
            destination: The local file path where the ZIP should be saved.

        Raises:
            SourceConnectionError: If the download fails.
        """
        logger.info(f"Downloading archive from {self.base_url} to {destination}")
        try:
            with requests.get(self.base_url, stream=True, timeout=60) as response:
                response.raise_for_status()
                with open(destination, "wb") as f:
                    for chunk in response.iter_content(chunk_size=self.CHUNK_SIZE):
                        f.write(chunk)
            logger.info("Download completed successfully.")
        except requests.RequestException as e:
            logger.error(f"Failed to download archive: {e}")
            raise SourceConnectionError(f"Failed to download from {self.base_url}: {e}") from e

    def extract_archive(self, zip_path: Path, destination_dir: Path) -> list[Path]:
        """
        Extract the ZIP archive to a destination directory safely.

        Args:
            zip_path: Path to the ZIP file.
            destination_dir: Directory where files should be extracted.

        Returns:
            A list of paths to the extracted files.

        Raises:
            SourceSchemaError: If the file is not a valid ZIP archive or contains unsafe paths.
        """
        logger.info(f"Extracting {zip_path} to {destination_dir}")

        if not zip_path.exists():
            raise SourceConnectionError(f"ZIP file not found at {zip_path}")

        extracted_files = []
        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                for member in zip_ref.infolist():
                    # Zip Slip protection
                    target_path = destination_dir / member.filename
                    # Resolve to absolute path to check it's within destination_dir
                    try:
                        resolved_path = target_path.resolve()
                        # On some systems, resolve() might fail if the path doesn't exist yet,
                        # but we can resolve the parent.
                        # A robust way is to check the common path.

                        # Simpler check: ensure the resolved path starts with the resolved destination dir
                        # Note: we need to handle the case where the file isn't created yet.
                        # So we rely on abspath logic or pathlib's resolve of the parent.

                        # Let's rely on string check for relative components first for simplicity in this context,
                        # but standard practice is realpath check.

                        if not resolved_path.is_relative_to(destination_dir.resolve()):
                            logger.warning(f"Skipping unsafe file path in zip: {member.filename}")
                            continue

                    except (ValueError, RuntimeError):
                        # is_relative_to can raise ValueError if on different drives (Windows)
                        logger.warning(f"Skipping invalid file path in zip: {member.filename}")
                        continue

                    zip_ref.extract(member, destination_dir)
                    extracted_files.append(target_path)

            logger.info(f"Extracted {len(extracted_files)} files.")
            return extracted_files
        except zipfile.BadZipFile as e:
            logger.error(f"Invalid ZIP file: {e}")
            raise SourceSchemaError(f"File at {zip_path} is not a valid ZIP archive.") from e

    def cleanup(self, path: Path) -> None:
        """
        Delete a file or directory.

        Args:
            path: Path to the file or directory to delete.
        """
        if not path.exists():
            return

        try:
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                shutil.rmtree(path)
            logger.debug(f"Cleaned up {path}")
        except OSError as e:
            logger.warning(f"Failed to cleanup {path}: {e}")

    def calculate_file_hash(self, file_path: Path) -> str:
        """
        Calculate the MD5 hash of a file.

        Args:
            file_path: Path to the file.

        Returns:
            The hexadecimal MD5 hash of the file content.

        Raises:
            OSError: If reading the file fails.
        """
        logger.debug(f"Calculating MD5 hash for {file_path}")
        file_hash = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                while chunk := f.read(self.CHUNK_SIZE):
                    file_hash.update(chunk)
            return file_hash.hexdigest()
        except OSError as e:
            logger.error(f"Failed to calculate hash for {file_path}: {e}")
            raise
