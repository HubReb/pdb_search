"""Unit tests for paper_sorts configuration (config.py and prompts.py helpers).

Tests cover: env var overrides, .env loading, Fernet source error path,
and empty-input re-prompt behaviour in cli/prompts.py.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from paper_sorts.config import Settings


def test_env_var_overrides_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """PDBSEARCH_DATABASE_URL environment variable overrides the default."""
    monkeypatch.setenv("PDBSEARCH_DATABASE_URL", "postgresql+psycopg://testhost/testdb")
    settings = Settings()
    assert settings.database_url == "postgresql+psycopg://testhost/testdb"


def test_log_level_validated(monkeypatch: pytest.MonkeyPatch) -> None:
    """Invalid log level raises ValidationError."""
    from pydantic import ValidationError

    monkeypatch.setenv("PDBSEARCH_LOG_LEVEL", "INVALID")
    with pytest.raises(ValidationError):
        Settings()


def test_log_level_case_insensitive(monkeypatch: pytest.MonkeyPatch) -> None:
    """Log level is normalised to uppercase."""
    monkeypatch.setenv("PDBSEARCH_LOG_LEVEL", "debug")
    settings = Settings()
    assert settings.log_level == "DEBUG"


def test_fernet_ini_source_missing_key_raises_clear_error() -> None:
    """FernetIniSettingsSource raises ValueError with plain-language message when key missing."""
    from paper_sorts.config import FernetIniSettingsSource

    with tempfile.NamedTemporaryFile(suffix=".crypt", delete=False) as f:
        f.write(b"not-real-encrypted-data")
        config_path = Path(f.name)
    missing_key = Path("/tmp/nonexistent_key_12345.key")

    try:
        with pytest.raises(ValueError, match="not found"):
            FernetIniSettingsSource(Settings, config_path, missing_key)
    finally:
        config_path.unlink(missing_ok=True)


def test_fernet_ini_source_invalid_data_raises_clear_error() -> None:
    """FernetIniSettingsSource raises ValueError when decryption fails."""
    from paper_sorts.config import FernetIniSettingsSource

    with (
        tempfile.NamedTemporaryFile(suffix=".crypt", delete=False) as cf,
        tempfile.NamedTemporaryFile(suffix=".key", delete=False) as kf,
    ):
        cf.write(b"not-real-encrypted-data")
        kf.write(b"not-a-real-fernet-key")
        config_path = Path(cf.name)
        key_path = Path(kf.name)

    try:
        with pytest.raises(ValueError):
            FernetIniSettingsSource(Settings, config_path, key_path)
    finally:
        config_path.unlink(missing_ok=True)
        key_path.unlink(missing_ok=True)


def test_settings_no_encrypted_config() -> None:
    """Settings initialises without encrypted config (config_path=None)."""
    settings = Settings(database_url="postgresql+psycopg://localhost/test")
    assert settings.database_url == "postgresql+psycopg://localhost/test"
    assert settings.config_path is None


def test_empty_input_reprompt() -> None:
    """ask_str re-prompts on empty input when required=True."""
    from io import StringIO

    # Simulate user entering empty then a real value
    inputs = iter(["", "  ", "real value"])

    with patch("paper_sorts.cli.prompts.Prompt.ask", side_effect=inputs):
        from paper_sorts.cli.prompts import ask_str

        result = ask_str("Test prompt", required=True)
    assert result == "real value"


def test_empty_input_allowed_when_not_required() -> None:
    """ask_str returns empty string when required=False."""
    with patch("paper_sorts.cli.prompts.Prompt.ask", return_value=""):
        from paper_sorts.cli.prompts import ask_str

        result = ask_str("Test prompt", required=False)
    assert result == ""
