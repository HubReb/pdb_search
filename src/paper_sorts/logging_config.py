"""Centralised logging configuration via :func:`logging.config.dictConfig`.

A single call to :func:`setup_logging` installs a ``RichHandler`` writing to stdout and,
optionally, a rotating-free ``FileHandler``. Per-class log files from the legacy tool are
superseded by this structured, configurable setup (the file sink is one option, not the only
sink).
"""

from __future__ import annotations

import logging
from logging.config import dictConfig


def setup_logging(level: str = "INFO", log_file: str | None = None) -> None:
    """Configure application logging once at startup.

    :param level: the root log level name (e.g. ``"INFO"``, ``"DEBUG"``).
    :param log_file: optional path for an additional file sink; ``None`` for stdout only.
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
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                }
            },
            "handlers": handlers,
            "root": {"level": level, "handlers": handler_names},
        }
    )
    logging.getLogger(__name__).debug("logging configured at level %s", level)
