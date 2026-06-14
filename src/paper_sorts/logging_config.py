"""Logging configuration for paper_sorts.

Provides a single configure_logging() function that sets up stdlib logging
using dictConfig.  Called once from cli/app.py at startup.

Sinks:
  - RichHandler to stdout (always enabled)
  - FileHandler to paper_sorts.log (optional; controlled by LOG_FILE env var)
"""

from __future__ import annotations

import logging
import logging.config
import os
from typing import Any


def configure_logging(level: str = "INFO") -> None:
    """Configure application-wide logging via dictConfig.

    Sets up a RichHandler on stdout.  If the environment variable
    PDBSEARCH_LOG_FILE is set, also writes to that file path.

    Args:
        level: Logging level string (DEBUG / INFO / WARNING / ERROR).
               Case-insensitive; defaults to INFO.
    """
    handlers: list[str] = ["rich_console"]
    handlers_config: dict[str, Any] = {
        "rich_console": {
            "class": "rich.logging.RichHandler",
            "level": level.upper(),
            "rich_tracebacks": False,
            "show_path": False,
        }
    }

    log_file = os.environ.get("PDBSEARCH_LOG_FILE")
    if log_file:
        handlers.append("file")
        handlers_config["file"] = {
            "class": "logging.FileHandler",
            "filename": log_file,
            "encoding": "utf-8",
            "level": level.upper(),
            "formatter": "plain",
        }

    config: dict[str, Any] = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "plain": {
                "format": "%(asctime)s %(name)s %(levelname)s %(message)s",
            }
        },
        "handlers": handlers_config,
        "root": {
            "level": level.upper(),
            "handlers": handlers,
        },
    }
    logging.config.dictConfig(config)
