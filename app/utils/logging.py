"""
Structured logging configuration for the application.
"""

import logging
import sys

from app.config import settings

_CONFIGURED = False


def get_logger(name: str) -> logging.Logger:
    """Return a named logger, configuring the root logger once."""
    global _CONFIGURED
    if not _CONFIGURED:
        level = getattr(logging, settings.log_level.upper(), logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        root = logging.getLogger()
        root.setLevel(level)
        if not root.handlers:
            root.addHandler(handler)
        _CONFIGURED = True
    return logging.getLogger(name)
