"""Logging configuration for paper_sorts.

Provides a single ``setup_logging`` function that configures the root logger
via ``logging.config.dictConfig``. Call it exactly once at application startup
from ``cli/app.py``.

Handlers:
- RichHandler to stdout (always enabled)
- FileHandler to a configurable log file (enabled when ``log_file`` is set)
"""

from __future__ import annotations

import logging
import logging.config
from pathlib import Path


def setup_logging(
    log_level: str = "INFO",
    log_file: Path | None = None,
) -> None:
    """Configure application logging via dictConfig.

    Sets up a RichHandler on stdout and, optionally, a FileHandler.
    Must be called exactly once at startup before any logging calls.

    :param log_level: log level string (e.g. 'INFO', 'DEBUG', 'WARNING')
    :type log_level: str
    :param log_file: optional path to write log entries to a file
    :type log_file: Path | None
    """
    handlers: dict[str, object] = {
        "rich": {
            "class": "rich.logging.RichHandler",
            "level": log_level,
            "rich_tracebacks": False,
            "show_path": False,
            "markup": False,
        }
    }
    handler_names = ["rich"]

    if log_file is not None:
        handlers["file"] = {
            "class": "logging.FileHandler",
            "filename": str(log_file),
            "level": log_level,
            "formatter": "standard",
            "encoding": "utf-8",
        }
        handler_names.append("file")

    config: dict[str, object] = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            }
        },
        "handlers": handlers,
        "loggers": {
            "paper_sorts": {
                "handlers": handler_names,
                "level": log_level,
                "propagate": False,
            }
        },
        "root": {
            "handlers": ["rich"],
            "level": "WARNING",
        },
    }
    logging.config.dictConfig(config)
