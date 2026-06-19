"""Single-call logging configuration.

A RichHandler writes human-readable logs to stdout; an optional FileHandler adds
a file sink when configured. This is invoked once at CLI startup, replacing the
legacy per-class file loggers.
"""

from __future__ import annotations

import logging
from logging.config import dictConfig


def configure_logging(level: str = "WARNING", log_file: str | None = None) -> None:
    """Configure root logging with a rich stdout handler and optional file sink.

    :param level: a logging level name (e.g. ``"DEBUG"``, ``"WARNING"``).
    :param log_file: optional path; when set, technical detail is also written
        there in addition to the rich stdout handler.
    """
    handlers: dict[str, dict[str, object]] = {
        "console": {
            "class": "rich.logging.RichHandler",
            "level": level,
            "rich_tracebacks": True,
            "show_path": False,
        }
    }
    active = ["console"]
    if log_file:
        handlers["file"] = {
            "class": "logging.FileHandler",
            "level": level,
            "filename": log_file,
            "formatter": "detailed",
        }
        active.append("file")

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
            "root": {"level": level, "handlers": active},
        }
    )
    logging.getLogger(__name__).debug("logging configured at level %s", level)
