"""Unit tests for paper_sorts.config.

Tests cover:
    - env var override
    - .env file loading
    - missing key file produces actionable error (not stack trace)
    - invalid log level rejected
    - valid log levels accepted
"""

from __future__ import annotations

from pathlib import Path

import pytest

from paper_sorts.config import Settings


def test_default_settings() -> None:
    """Settings can be instantiated with no arguments."""
    s = Settings()
    assert s.log_level == "INFO"
    assert s.section == "postgresql"


def test_env_var_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """Environment variables with PDBSEARCH_ prefix override defaults."""
    monkeypatch.setenv("PDBSEARCH_DATABASE_URL", "postgresql+psycopg://user:pass@host/db")
    monkeypatch.setenv("PDBSEARCH_LOG_LEVEL", "DEBUG")
    s = Settings()
    assert s.database_url == "postgresql+psycopg://user:pass@host/db"
    assert s.log_level == "DEBUG"


def test_dotenv_loading(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Settings reads database_url from a .env file."""
    env_file = tmp_path / ".env"
    env_file.write_text("PDBSEARCH_DATABASE_URL=postgresql+psycopg://dotenv@host/db\n")
    # Change cwd so pydantic-settings finds the .env file.
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("PDBSEARCH_DATABASE_URL", raising=False)
    s = Settings()
    assert s.database_url == "postgresql+psycopg://dotenv@host/db"


def test_invalid_log_level_rejected() -> None:
    """Settings raises ValueError for an unrecognised log level."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Settings(log_level="VERBOSE")  # type: ignore[call-arg]


def test_valid_log_levels_accepted() -> None:
    """All standard log level strings are accepted."""
    for level in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
        s = Settings(log_level=level)  # type: ignore[call-arg]
        assert s.log_level == level


def test_missing_key_file_returns_empty_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When the key file does not exist, the Fernet source returns empty (no crash).

    The encrypted config source should silently skip when either file is absent
    (per-spec: actionable error only when the file *exists* but cannot be decrypted).
    """
    monkeypatch.delenv("PDBSEARCH_DATABASE_URL", raising=False)
    monkeypatch.chdir(tmp_path)
    # No config or key file present in tmp_path.
    s = Settings(config=str(tmp_path / "does_not_exist.crypt"), key=str(tmp_path / "no_key"))
    # Should not crash; database_url stays empty.
    assert s.database_url == ""


def test_get_log_level_int() -> None:
    """get_log_level_int returns the correct integer for the configured level."""
    import logging

    s = Settings(log_level="DEBUG")  # type: ignore[call-arg]
    assert s.get_log_level_int() == logging.DEBUG

    s2 = Settings(log_level="WARNING")  # type: ignore[call-arg]
    assert s2.get_log_level_int() == logging.WARNING
