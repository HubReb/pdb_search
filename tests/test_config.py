"""Unit tests for paper_sorts configuration (config.py and prompts.py helpers).

Tests cover: env var overrides, .env loading, Fernet source error path,
and empty-input re-prompt behaviour in cli/prompts.py.
"""

from __future__ import annotations

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


def test_ask_int_valid_choice() -> None:
    """ask_int returns the integer when user enters a valid choice."""
    with patch("paper_sorts.cli.prompts.Prompt.ask", return_value="2"):
        from paper_sorts.cli.prompts import ask_int

        result = ask_int("Choose", [1, 2, 3])
    assert result == 2


def test_ask_int_invalid_then_valid() -> None:
    """ask_int re-prompts on invalid input and returns valid choice."""
    inputs = iter(["0", "abc", "3"])
    with patch("paper_sorts.cli.prompts.Prompt.ask", side_effect=inputs):
        from paper_sorts.cli.prompts import ask_int

        result = ask_int("Choose", [1, 2, 3])
    assert result == 3


def test_ask_confirm_yes_variants() -> None:
    """ask_confirm returns True for y, yes, 1."""
    for answer in ["y", "yes", "Y", "YES", "1"]:
        with patch("paper_sorts.cli.prompts.Prompt.ask", return_value=answer):
            from paper_sorts.cli.prompts import ask_confirm

            assert ask_confirm("Confirm?") is True


def test_ask_confirm_no_variants() -> None:
    """ask_confirm returns False for n, no, 2."""
    for answer in ["n", "no", "N", "NO", "2"]:
        with patch("paper_sorts.cli.prompts.Prompt.ask", return_value=answer):
            from paper_sorts.cli.prompts import ask_confirm

            assert ask_confirm("Confirm?") is False


def test_ask_confirm_invalid_then_valid() -> None:
    """ask_confirm re-prompts on invalid input."""
    inputs = iter(["maybe", "y"])
    with patch("paper_sorts.cli.prompts.Prompt.ask", side_effect=inputs):
        from paper_sorts.cli.prompts import ask_confirm

        result = ask_confirm("Confirm?")
    assert result is True


def test_configure_logging_runs() -> None:
    """configure_logging can be called without error."""
    from paper_sorts.logging_config import configure_logging

    configure_logging("DEBUG")
    configure_logging("WARNING")


def test_fernet_ini_source_none_paths() -> None:
    """FernetIniSettingsSource with None paths returns empty data."""
    from paper_sorts.config import FernetIniSettingsSource

    source = FernetIniSettingsSource(Settings, None, None)
    assert source() == {}


def test_fernet_ini_source_valid_config() -> None:
    """FernetIniSettingsSource successfully decrypts a valid Fernet-encrypted INI."""
    from cryptography.fernet import Fernet

    from paper_sorts.config import FernetIniSettingsSource

    # Generate a real Fernet key and encrypt a valid INI
    key = Fernet.generate_key()
    ini_content = "[postgresql]\ndbname=testdb\nuser=testuser\npassword=testpass\nhost=localhost\nport=5432\n"
    encrypted = Fernet(key).encrypt(ini_content.encode())

    with (
        tempfile.NamedTemporaryFile(suffix=".crypt", delete=False) as cf,
        tempfile.NamedTemporaryFile(suffix=".key", delete=False) as kf,
    ):
        cf.write(encrypted)
        kf.write(key)
        config_path = Path(cf.name)
        key_path = Path(kf.name)

    try:
        source = FernetIniSettingsSource(Settings, config_path, key_path)
        data = source()
        assert "database_url" in data
        assert "testdb" in data["database_url"]
        assert "testuser" in data["database_url"]
    finally:
        config_path.unlink(missing_ok=True)
        key_path.unlink(missing_ok=True)


def test_fernet_ini_source_get_field_value() -> None:
    """FernetIniSettingsSource.get_field_value returns value for known field."""
    from paper_sorts.config import FernetIniSettingsSource

    source = FernetIniSettingsSource(Settings, None, None)
    # With no config, all fields return None
    value, name, is_complex = source.get_field_value(None, "database_url")  # type: ignore[arg-type]
    assert value is None
    assert name == "database_url"
    assert is_complex is False
