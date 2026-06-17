"""Centralised logging configuration via :func:`logging.config.dictConfig`.

A single :class:`rich.logging.RichHandler` writes to stdout by default; an
optional :class:`logging.FileHandler` is added when a log file is configured.
This replaces the legacy per-class file loggers with configurable sinks.
"""

from __future__ import annotations

import logging.config


def setup_logging(level: str = "INFO", log_file: str | None = None) -> None:
    """Configure root logging once at startup.

    :param level: logging level name (e.g. ``"DEBUG"``, ``"INFO"``).
    :param log_file: optional path for an additional file sink.
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
            "formatter": "plain",
        }
        handler_names.append("file")

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "plain": {"format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"}
            },
            "handlers": handlers,
            "root": {"level": level, "handlers": handler_names},
        }
    )
