"""Centralised logging configuration.

A single :func:`logging.config.dictConfig` call wires a Rich-formatted handler
to stdout and, optionally, a file handler. This replaces the legacy per-class
``FileHandler`` factories with one configurable sink (FR-013).
"""

from __future__ import annotations

import logging
from logging.config import dictConfig


def configure_logging(level: str = "INFO", log_file: str | None = None) -> None:
    """Configure root logging with a RichHandler and an optional file sink.

    :param level: the log level name (e.g. ``"DEBUG"``, ``"INFO"``).
    :param log_file: if given, also write logs to this file; otherwise stdout only.
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
    if log_file is not None:
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
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                }
            },
            "handlers": handlers,
            "root": {"level": level, "handlers": handler_names},
        }
    )


def get_logger(name: str) -> logging.Logger:
    """Return a named logger.

    :param name: the logger name (typically ``__name__``).
    :return: the configured :class:`logging.Logger`.
    """
    return logging.getLogger(name)
