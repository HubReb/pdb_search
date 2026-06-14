"""Centralised logging configuration for paper_sorts.

Called once from cli/app.py at application startup. Sets up a RichHandler
to stdout and an optional FileHandler if PDBSEARCH_LOG_FILE is set.

All logging in the application goes through the stdlib logging module;
this module is the single place that configures handlers and formatters.
"""

import logging
import logging.config
import os


def setup_logging(log_level: str = "INFO") -> None:
    """Configure application-wide logging using dictConfig.

    Sets up:
    - A RichHandler writing to stdout at the requested level.
    - An optional FileHandler writing to PDBSEARCH_LOG_FILE (if set).

    Args:
        log_level: Python logging level name (e.g. "DEBUG", "INFO").
            Case-insensitive. Defaults to "INFO".

    Returns:
        None
    """
    level = log_level.upper()
    log_file = os.environ.get("PDBSEARCH_LOG_FILE")

    handlers: dict[str, object] = {
        "rich": {
            "class": "rich.logging.RichHandler",
            "level": level,
            "rich_tracebacks": False,
            "show_path": False,
            "formatter": "rich_fmt",
        }
    }

    if log_file:
        handlers["file"] = {
            "class": "logging.FileHandler",
            "filename": log_file,
            "level": level,
            "formatter": "plain_fmt",
        }

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "rich_fmt": {
                    "format": "%(message)s",
                    "datefmt": "[%X]",
                },
                "plain_fmt": {
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                },
            },
            "handlers": handlers,
            "root": {
                "level": level,
                "handlers": list(handlers.keys()),
            },
        }
    )
