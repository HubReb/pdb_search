"""Single-call logging setup driven by :class:`paper_sorts.config.Settings`.

Replaces the legacy ``helpers.create_logger`` per-class file-logger pattern
(which produced one ``*_logger.log`` file per class instance, six per run)
with one :func:`logging.config.dictConfig` invocation:

* :class:`rich.logging.RichHandler` writes structured/coloured records to
  stdout at ``settings.log_level``.
* A :class:`logging.FileHandler` is attached only when
  ``settings.log_file`` is set (default: stdout-only).

The config is applied to the root logger so every module-level
``logging.getLogger(__name__)`` call inherits the same sinks.
"""

from __future__ import annotations

import logging.config
from typing import Any

from paper_sorts.config import Settings


def configure_logging(settings: Settings) -> None:
    """Apply the dict-config derived from ``settings``.

    Calling more than once replaces the prior handlers cleanly thanks to
    ``disable_existing_loggers=False`` and dictConfig's reset semantics on
    the root logger.

    Args:
        settings: Resolved runtime settings; only ``log_level`` and
            ``log_file`` are consulted.
    """
    handlers: dict[str, dict[str, Any]] = {
        "console": {
            "class": "rich.logging.RichHandler",
            "level": settings.log_level,
            "formatter": "rich",
            "rich_tracebacks": True,
            "show_time": True,
            "show_path": False,
        },
    }

    if settings.log_file is not None:
        handlers["file"] = {
            "class": "logging.FileHandler",
            "level": settings.log_level,
            "formatter": "file",
            "filename": str(settings.log_file),
            "encoding": "utf-8",
        }

    config: dict[str, Any] = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "rich": {"format": "%(message)s", "datefmt": "[%X]"},
            "file": {
                "format": "%(asctime)s %(levelname)-7s %(name)s: %(message)s",
            },
        },
        "handlers": handlers,
        "root": {
            "level": settings.log_level,
            "handlers": list(handlers.keys()),
        },
    }

    logging.config.dictConfig(config)
