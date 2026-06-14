"""Logging configuration for paper_sorts.

Provides a single :func:`setup_logging` call that configures Python's stdlib
``logging`` via ``dictConfig``.  A ``RichHandler`` is always attached to
``stdout``; an optional file handler can be added by passing ``log_file``.
Call once from :mod:`paper_sorts.cli.app` at startup.
"""

from __future__ import annotations

import logging
import logging.config
from pathlib import Path


def setup_logging(level: str = "INFO", log_file: Path | None = None) -> None:
    """Configure stdlib logging with a RichHandler and an optional FileHandler.

    :param level: Logging level string (e.g. ``"INFO"``, ``"DEBUG"``).
        Case-insensitive.
    :param log_file: Optional path to a log file.  If given, a ``FileHandler``
        is added alongside the console handler.
    :raises ValueError: If ``level`` is not a valid Python logging level name.
    """
    handlers: dict[str, object] = {
        "rich": {
            "class": "rich.logging.RichHandler",
            "level": level.upper(),
            "rich_tracebacks": False,
            "show_path": False,
            "formatter": "plain",
        }
    }
    handler_names = ["rich"]

    if log_file is not None:
        handlers["file"] = {
            "class": "logging.FileHandler",
            "level": level.upper(),
            "filename": str(log_file),
            "encoding": "utf-8",
            "formatter": "detailed",
        }
        handler_names.append("file")

    config: dict[str, object] = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "plain": {
                "format": "%(message)s",
            },
            "detailed": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            },
        },
        "handlers": handlers,
        "root": {
            "level": level.upper(),
            "handlers": handler_names,
        },
    }
    logging.config.dictConfig(config)
