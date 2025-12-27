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
import os
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
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                logger.error(f"Download link not found (404): {self.base_url}")
                raise SourceSchemaError(f"Download link not found: {self.base_url}") from e
            logger.error(f"HTTP error during download: {e}")
            raise SourceConnectionError(f"HTTP error downloading from {self.base_url}: {e}") from e
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
                    # Zip Slip protection: ensure target path is within destination dir
                    try:
                        # Use os.path.realpath to resolve any symlinks/..'s
                        # We must resolve destination_dir to its canonical path
                        real_destination = os.path.realpath(destination_dir)

                        # Join the destination with the filename
                        # os.path.join handles absolute paths in member.filename by discarding previous part
                        # so we must be careful, but we check the result afterwards.
                        target_path_str = os.path.join(real_destination, member.filename)
                        real_target = os.path.realpath(target_path_str)

                        # Check if the resolved target starts with the resolved destination
                        # os.path.commonpath correctly handles path components
                        common_prefix = os.path.commonpath([real_destination, real_target])
                        if common_prefix != real_destination:
                            logger.warning(f"Skipping unsafe file path in zip: {member.filename}")
                            continue

                        # If safe, we extract
                        zip_ref.extract(member, destination_dir)
                        # We return the path object corresponding to where it was extracted
                        # We use the original destination_dir / member.filename structure for the return
                        extracted_files.append(destination_dir / member.filename)

                    except (ValueError, RuntimeError, OSError) as e:
                        logger.warning(f"Skipping invalid file path in zip ({member.filename}): {e}")
                        continue

            logger.info(f"Extracted {len(extracted_files)} files.")
            return extracted_files
        except zipfile.BadZipFile as e:
            logger.error(f"Invalid ZIP file: {e}")
            raise SourceSchemaError(f"File at {zip_path} is not a valid ZIP archive.") from e

    def calculate_file_hash(self, file_path: Path) -> str:
        """
        Calculate the MD5 hash of a file.

        Args:
            file_path: Path to the file.

        Returns:
            The MD5 hex digest of the file.

        Raises:
            SourceConnectionError: If the file cannot be read.
        """
        logger.debug(f"Calculating hash for {file_path}")
        if not file_path.exists():
            raise SourceConnectionError(f"File not found for hashing: {file_path}")

        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(self.CHUNK_SIZE), b""):
                    hash_md5.update(chunk)
            digest = hash_md5.hexdigest()
            logger.debug(f"Hash for {file_path.name}: {digest}")
            return digest
        except OSError as e:
            logger.error(f"Failed to calculate hash for {file_path}: {e}")
            raise SourceConnectionError(f"Failed to calculate hash for {file_path}: {e}") from e

    def resolve_product_files(self, extracted_files: list[Path]) -> dict[str, list[Path]]:
        """
        Map extracted files to logical roles (products, patent, exclusivity).

        Args:
            extracted_files: List of paths to the extracted files.

        Returns:
            A dictionary where keys are logical roles ('products', 'patent', 'exclusivity')
            and values are lists of corresponding file paths.

        Raises:
            SourceSchemaError: If critical files (products or components) are missing.
        """
        mapping: dict[str, list[Path]] = {
            "products": [],
            "patent": [],
            "exclusivity": [],
        }

        # Create a lookup for case-insensitive matching
        name_map = {f.name.lower(): f for f in extracted_files}

        # 1. Resolve Products
        if FdaConfig.FILE_PRODUCTS.lower() in name_map:
            mapping["products"].append(name_map[FdaConfig.FILE_PRODUCTS.lower()])
        else:
            # Fallback to components
            components = ["rx.txt", "otc.txt", "disc.txt"]
            found_components = []
            for comp in components:
                if comp in name_map:
                    found_components.append(name_map[comp])

            if found_components:
                mapping["products"].extend(found_components)
            else:
                logger.error("No valid product files found (products.txt or rx/otc/disc.txt)")
                raise SourceSchemaError("Missing required product files (products.txt or rx.txt/otc.txt/disc.txt)")

        # 2. Resolve Patent
        if FdaConfig.FILE_PATENTS.lower() in name_map:
            mapping["patent"].append(name_map[FdaConfig.FILE_PATENTS.lower()])
        else:
            logger.warning("patent.txt not found in extracted files.")

        # 3. Resolve Exclusivity
        if FdaConfig.FILE_EXCLUSIVITY.lower() in name_map:
            mapping["exclusivity"].append(name_map[FdaConfig.FILE_EXCLUSIVITY.lower()])
        else:
            logger.warning("exclusivity.txt not found in extracted files.")

        return mapping

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
