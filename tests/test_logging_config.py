"""Tests for :func:`paper_sorts.logging_config.setup_logging`.

Covers the default stdout-only configuration and the optional file sink so both
handler branches are exercised.
"""

from __future__ import annotations

import logging
from pathlib import Path

from paper_sorts.logging_config import setup_logging


def test_setup_logging_console_only() -> None:
    """The default configuration sets the root level and a console handler."""
    setup_logging("DEBUG")
    assert logging.getLogger().level == logging.DEBUG


def test_setup_logging_with_file_sink(tmp_path: Path) -> None:
    """A configured log file adds a working file handler."""
    log_file = tmp_path / "app.log"
    setup_logging("INFO", str(log_file))
    logging.getLogger("test.file.sink").info("hello from the file sink")
    for handler in logging.getLogger().handlers:
        handler.flush()
    assert log_file.exists()
    assert "hello from the file sink" in log_file.read_text(encoding="utf-8")
    # Restore the default console-only configuration for later tests.
    setup_logging("INFO")
