"""Centralised logging configuration for paper_sorts.

Call :func:`configure_logging` once at CLI startup.  After that, any module
can use ``logging.getLogger(__name__)`` and inherit the configured handlers.
"""

from __future__ import annotations

import logging
import logging.config


def configure_logging(log_level: str = "INFO") -> None:
    """Configure application-wide logging via :func:`logging.config.dictConfig`.

    Sets up a :class:`rich.logging.RichHandler` for colourised stdout output
    and optionally a plain :class:`~logging.FileHandler` if the level is
    ``DEBUG``.  The configuration is applied to the root logger so all
    ``logging.getLogger(__name__)`` calls across the package inherit it.

    :param log_level: A valid logging level name (case-insensitive).
        Accepted values: ``DEBUG``, ``INFO``, ``WARNING``, ``ERROR``,
        ``CRITICAL``.  Defaults to ``"INFO"``.
    """
    level = log_level.upper()
    numeric_level = getattr(logging, level, logging.INFO)

    handlers: dict[str, object] = {
        "rich": {
            "class": "rich.logging.RichHandler",
            "level": level,
            "rich_tracebacks": False,
            "show_path": False,
            "markup": False,
        },
    }

    handler_names = ["rich"]

    if numeric_level == logging.DEBUG:
        handlers["file"] = {
            "class": "logging.FileHandler",
            "filename": "pdbsearch.log",
            "level": "DEBUG",
            "formatter": "plain",
        }
        handler_names.append("file")

    config: dict[str, object] = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "plain": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            },
        },
        "handlers": handlers,
        "root": {
            "level": level,
            "handlers": handler_names,
        },
        # Quiet noisy third-party loggers
        "loggers": {
            "sqlalchemy.engine": {"level": "WARNING"},
            "alembic": {"level": "WARNING"},
        },
    }

    logging.config.dictConfig(config)
