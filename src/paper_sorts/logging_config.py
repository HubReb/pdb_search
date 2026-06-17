"""Single-point logging configuration for the application.

A lone :func:`logging.config.dictConfig` wires a Rich handler to stdout and,
when a log-file path is configured, an optional file handler. This replaces the
per-class ``*.log`` files of the legacy stack. It is called once from
:mod:`paper_sorts.cli.app` at startup.
"""

from __future__ import annotations

import logging.config

LOGGER_NAME = "paper_sorts"


def configure_logging(level: str = "INFO", log_file: str | None = None) -> None:
    """Configure the application logger with a Rich stdout sink (+ optional file).

    :param level: the logging level name (``DEBUG``/``INFO``/``WARNING``/
        ``ERROR``); invalid names fall back to ``INFO``.
    :param log_file: optional path; when set, a file handler is added alongside
        the Rich stdout handler.
    """
    level_name = level.upper()
    if level_name not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        level_name = "INFO"

    handlers: dict[str, dict[str, object]] = {
        "console": {
            "class": "rich.logging.RichHandler",
            "level": level_name,
            "rich_tracebacks": True,
            "show_path": False,
        }
    }
    handler_names = ["console"]

    if log_file:
        handlers["file"] = {
            "class": "logging.FileHandler",
            "level": level_name,
            "filename": log_file,
            "formatter": "detailed",
        }
        handler_names.append("file")

    logging.config.dictConfig(
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
                    "level": level_name,
                    "handlers": handler_names,
                    "propagate": False,
                }
            },
        }
    )


def get_logger() -> logging.Logger:
    """Return the application logger.

    :returns: the ``paper_sorts`` named logger.
    """
    return logging.getLogger(LOGGER_NAME)
