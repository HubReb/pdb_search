"""Logging configuration for paper-sorts.

Call :func:`configure_logging` once at application startup (in
``cli/app.py``) to set up a RichHandler on stdout and an optional
file handler.

Usage::

    from paper_sorts.logging_config import configure_logging

    configure_logging("INFO")
"""

from __future__ import annotations

import logging
import logging.config
from typing import Any


def configure_logging(log_level: str = "INFO", log_file: str | None = None) -> None:
    """Configure application-wide logging via :func:`logging.config.dictConfig`.

    Sets up a :class:`rich.logging.RichHandler` to stdout for human-readable
    output, and optionally a :class:`logging.FileHandler` when *log_file* is
    given.  Called once from ``cli/app.py`` at startup; subsequent calls are
    idempotent (root logger already has handlers).

    :param log_level: Logging level string (``"DEBUG"``, ``"INFO"``, etc.).
        Case-insensitive.  Defaults to ``"INFO"``.
    :param log_file: Optional path for a supplementary file log.  When
        *None* (the default), no file handler is added.
    :raises ValueError: If *log_level* is not a recognised logging level.
    """
    level_upper = log_level.upper()
    numeric = getattr(logging, level_upper, None)
    if not isinstance(numeric, int):
        raise ValueError(f"Unknown log level: {log_level!r}")

    handlers: dict[str, Any] = {
        "rich": {
            "class": "rich.logging.RichHandler",
            "level": level_upper,
            "rich_tracebacks": False,
            "show_path": False,
        }
    }
    handler_names = ["rich"]

    if log_file:
        handlers["file"] = {
            "class": "logging.FileHandler",
            "filename": log_file,
            "level": level_upper,
            "formatter": "standard",
            "encoding": "utf-8",
        }
        handler_names.append("file")

    config: dict[str, Any] = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            }
        },
        "handlers": handlers,
        "root": {
            "level": level_upper,
            "handlers": handler_names,
        },
    }
    logging.config.dictConfig(config)
