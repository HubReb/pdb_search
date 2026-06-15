"""Logging configuration for paper_sorts.

Sets up structured logging with a RichHandler to stdout and an optional
FileHandler. Call configure_logging() once at startup from cli/app.py.
"""

from __future__ import annotations

import logging
import logging.config
from pathlib import Path


def configure_logging(level: str = "INFO", log_file: Path | None = None) -> None:
    """Configure application-wide logging using dictConfig.

    Sets up a RichHandler for stdout and, optionally, a FileHandler.
    This function MUST be called exactly once at application startup
    (from cli/app.py) before any logger is used.

    :param level: Logging level name (e.g. 'INFO', 'DEBUG', 'WARNING').
    :param log_file: Optional path to write log output to in addition to stdout.
    :raises ValueError: If level is not a valid logging level name.
    """
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(
            f"Invalid log level '{level}'. Choose from: DEBUG, INFO, WARNING, ERROR, CRITICAL."
        )

    handlers: dict[str, object] = {
        "rich": {
            "class": "rich.logging.RichHandler",
            "level": level.upper(),
            "rich_tracebacks": False,
            "show_time": True,
            "show_level": True,
            "show_path": False,
        }
    }
    handler_names = ["rich"]

    if log_file is not None:
        handlers["file"] = {
            "class": "logging.FileHandler",
            "level": level.upper(),
            "filename": str(log_file),
            "formatter": "standard",
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
        "root": {
            "level": level.upper(),
            "handlers": handler_names,
        },
        "loggers": {
            "paper_sorts": {
                "level": level.upper(),
                "handlers": handler_names,
                "propagate": False,
            },
            "sqlalchemy.engine": {
                "level": "WARNING",
                "handlers": handler_names,
                "propagate": False,
            },
            "alembic": {
                "level": "INFO",
                "handlers": handler_names,
                "propagate": False,
            },
        },
    }
    logging.config.dictConfig(config)
