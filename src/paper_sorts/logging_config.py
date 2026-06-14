"""Logging configuration for paper_sorts.

Called once at startup from cli/app.py via configure_logging().
Uses dictConfig with RichHandler (stdout) and an optional FileHandler.
"""

import logging
import logging.config


def configure_logging(level: str = "INFO", log_file: str | None = None) -> None:
    """Configure application-wide logging.

    Sets up a RichHandler for stdout and optionally a FileHandler.
    Should be called exactly once at process startup.

    :param level: logging level string, e.g. "DEBUG", "INFO", "WARNING"
    :param log_file: optional path to a log file; if None only stdout handler is used
    """
    handlers: dict[str, object] = {
        "rich": {
            "class": "rich.logging.RichHandler",
            "level": level,
            "rich_tracebacks": False,
            "show_path": False,
            "markup": False,
        }
    }
    root_handlers = ["rich"]

    if log_file:
        handlers["file"] = {
            "class": "logging.FileHandler",
            "filename": log_file,
            "level": level,
            "formatter": "detailed",
        }
        root_handlers.append("file")

    config: dict[str, object] = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "detailed": {
                "format": "%(asctime)s %(name)s %(levelname)s %(message)s",
            },
        },
        "handlers": handlers,
        "root": {
            "level": level,
            "handlers": root_handlers,
        },
    }
    logging.config.dictConfig(config)
