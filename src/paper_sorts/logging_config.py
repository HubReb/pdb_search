"""Logging configuration for paper_sorts.

Call :func:`setup` exactly once at application startup (from ``cli/app.py``).
All subsequent calls to ``logging.getLogger(name)`` will pick up the handlers
configured here.
"""

from __future__ import annotations

import logging
import logging.config
from pathlib import Path


def setup(log_level: int = logging.INFO, log_file: str | Path | None = None) -> None:
    """Configure application-wide logging.

    Installs a RichHandler on stdout at the requested level and, optionally,
    a FileHandler writing to *log_file*.  Should be called exactly once from
    the CLI entry point before any other logging calls.

    :param log_level: stdlib logging level integer (e.g. ``logging.INFO``).
    :param log_file: optional file path to write structured log lines to.
        If ``None``, only the stdout handler is installed.
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
            "level": logging.DEBUG,
            "filename": str(log_file),
            "encoding": "utf-8",
            "formatter": "detailed",
        }
        handler_names.append("file")

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "detailed": {
                    "format": "%(asctime)s %(levelname)-8s %(name)s %(message)s",
                    "datefmt": "%Y-%m-%dT%H:%M:%S",
                }
            },
            "handlers": handlers,
            "root": {
                "level": log_level,
                "handlers": handler_names,
            },
            # Suppress noisy SQLAlchemy engine logs unless DEBUG is requested.
            "loggers": {
                "sqlalchemy.engine": {
                    "level": logging.WARNING if log_level > logging.DEBUG else logging.DEBUG,
                    "handlers": handler_names,
                    "propagate": False,
                }
            },
        }
    )
