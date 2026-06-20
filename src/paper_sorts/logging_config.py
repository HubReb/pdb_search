"""Logging configuration for paper_sorts.

Called once at application startup from ``cli/app.py``.  Configures a
``RichHandler`` for stdout output and an optional file handler.
"""

import logging
import logging.config
from pathlib import Path


def configure_logging(
    level: str = "INFO",
    log_file: Path | None = None,
) -> None:
    """Configure application logging via ``logging.config.dictConfig``.

    :param level: Log level string (DEBUG, INFO, WARNING, ERROR).
        Case-insensitive.
    :param log_file: Optional path for a secondary file handler.  When
        ``None`` (default) only the stdout RichHandler is configured.
    """
    level_upper = level.upper()

    handlers: dict[str, dict[str, object]] = {
        "rich": {
            "class": "rich.logging.RichHandler",
            "level": level_upper,
            "rich_tracebacks": False,
            "show_path": False,
            "markup": False,
        }
    }
    root_handlers = ["rich"]

    if log_file is not None:
        handlers["file"] = {
            "class": "logging.FileHandler",
            "filename": str(log_file),
            "level": level_upper,
            "formatter": "standard",
        }
        root_handlers.append("file")

    config: dict[str, object] = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            }
        },
        "handlers": handlers,
        "root": {
            "handlers": root_handlers,
            "level": level_upper,
        },
        "loggers": {
            "sqlalchemy.engine": {
                "level": "WARNING",
                "propagate": True,
            },
        },
    }

    logging.config.dictConfig(config)
