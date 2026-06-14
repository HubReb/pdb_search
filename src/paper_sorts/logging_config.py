"""Logging configuration for paper_sorts.

Called once at startup from cli/app.py. Configures a RichHandler to stdout
(INFO+) and an optional FileHandler if PDBSEARCH_LOG_FILE is set.
"""

import logging
import logging.config
import os


def configure_logging(level: str = "INFO") -> None:
    """Configure application-wide logging.

    :param level: Python logging level name (e.g. "INFO", "DEBUG", "WARNING").
    :raises ValueError: if level is not a valid logging level name.
    """
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {level!r}")

    handlers: dict[str, object] = {
        "rich": {
            "class": "rich.logging.RichHandler",
            "level": level.upper(),
            "rich_tracebacks": False,
            "show_path": False,
            "markup": False,
        }
    }
    handler_names = ["rich"]

    log_file = os.environ.get("PDBSEARCH_LOG_FILE")
    if log_file:
        handlers["file"] = {
            "class": "logging.FileHandler",
            "filename": log_file,
            "level": level.upper(),
            "formatter": "detailed",
        }
        handler_names.append("file")

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "detailed": {
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                }
            },
            "handlers": handlers,
            "root": {
                "level": level.upper(),
                "handlers": handler_names,
            },
        }
    )
