"""Centralized logging configuration.

A single :func:`logging.config.dictConfig` call wires a ``RichHandler`` to
stdout and, optionally, a ``FileHandler``. Called once from ``cli/app.py`` at
startup. This supersedes the legacy per-class log files (FR-013); the file sink
remains available as a configuration option but is no longer the only sink.

Failure paths log full technical detail here while the CLI surfaces a short,
plain-language message (Principle III).
"""

from __future__ import annotations

import logging
from logging.config import dictConfig

LOGGER_NAME = "paper_sorts"


def configure_logging(level: str = "INFO", log_file: str | None = None) -> logging.Logger:
    """Configure and return the application logger.

    :param level: stdlib level name (``DEBUG``/``INFO``/``WARNING``/...).
    :param log_file: optional path; when set, a ``FileHandler`` is added
        alongside the stdout ``RichHandler``.
    :returns: the configured ``paper_sorts`` logger.
    """
    handlers: dict[str, dict[str, object]] = {
        "console": {
            "class": "rich.logging.RichHandler",
            "level": level,
            "rich_tracebacks": True,
            "show_path": False,
        }
    }
    handler_names = ["console"]
    if log_file:
        handlers["file"] = {
            "class": "logging.FileHandler",
            "level": level,
            "filename": log_file,
            "formatter": "detailed",
        }
        handler_names.append("file")

    dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "detailed": {
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                }
            },
            "handlers": handlers,
            "loggers": {
                LOGGER_NAME: {
                    "handlers": handler_names,
                    "level": level,
                    "propagate": False,
                }
            },
        }
    )
    return logging.getLogger(LOGGER_NAME)
