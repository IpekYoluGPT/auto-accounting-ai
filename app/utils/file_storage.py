"""
Temporary file storage helpers.
"""

import os
import uuid
from pathlib import Path

from app.config import settings
from app.utils.logging import get_logger

logger = get_logger(__name__)


def get_storage_path(subfolder: str = "media") -> Path:
    """Return (and create) a storage sub-directory."""
    path = Path(settings.storage_dir) / subfolder
    path.mkdir(parents=True, exist_ok=True)
    return path


def save_temp_file(content: bytes, extension: str = "bin", subfolder: str = "media") -> Path:
    """Save *content* to a uniquely named temp file and return its Path."""
    directory = get_storage_path(subfolder)
    filename = f"{uuid.uuid4().hex}.{extension}"
    filepath = directory / filename
    filepath.write_bytes(content)
    logger.debug("Saved temp file: %s (%d bytes)", filepath, len(content))
    return filepath


def cleanup_file(filepath: Path) -> None:
    """Delete *filepath* if it exists, ignoring errors."""
    try:
        if filepath.exists():
            filepath.unlink()
            logger.debug("Removed temp file: %s", filepath)
    except OSError as exc:
        logger.warning("Could not remove temp file %s: %s", filepath, exc)
