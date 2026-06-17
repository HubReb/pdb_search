"""Tests for the configuration layer: priority chain and encrypted-INI source."""

from __future__ import annotations

from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from paper_sorts.config import (
    ConfigError,
    Settings,
    load_encrypted_database_url,
    load_settings,
)


def _write_encrypted_ini(tmp_path: Path) -> tuple[Path, Path]:
    """Write an encrypted INI + key file and return their paths.

    :param tmp_path: pytest temp directory.
    :returns: ``(config_path, key_path)``.
    """
    key = Fernet.generate_key()
    ini = (
        "[postgresql]\n"
        "host=db.example\nport=5432\nuser=alice\npassword=secret\ndbname=papers\n"
    )
    config_path = tmp_path / "database.crypt"
    key_path = tmp_path / "key"
    config_path.write_bytes(Fernet(key).encrypt(ini.encode("utf-8")))
    key_path.write_bytes(key)
    return config_path, key_path


def test_cli_override_beats_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """A CLI ``database_url`` override wins over the environment variable."""
    monkeypatch.setenv("PDBSEARCH_DATABASE_URL", "postgresql+psycopg://env/db")
    settings = load_settings(database_url="postgresql+psycopg://cli/db")
    assert settings.database_url == "postgresql+psycopg://cli/db"


def test_env_used_when_no_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    """The environment variable is used when no CLI override is given."""
    monkeypatch.setenv("PDBSEARCH_DATABASE_URL", "postgresql+psycopg://env/db")
    settings = load_settings(database_url=None)
    assert settings.database_url == "postgresql+psycopg://env/db"


def test_encrypted_ini_builds_url(tmp_path: Path) -> None:
    """The encrypted INI decrypts into a well-formed database URL."""
    config_path, key_path = _write_encrypted_ini(tmp_path)
    url = load_encrypted_database_url(config_path, key_path)
    assert url == "postgresql+psycopg://alice:secret@db.example:5432/papers"


def test_encrypted_ini_is_lowest_priority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The encrypted source supplies the URL only when no higher source does."""
    monkeypatch.delenv("PDBSEARCH_DATABASE_URL", raising=False)
    config_path, key_path = _write_encrypted_ini(tmp_path)
    settings = load_settings(config_file=str(config_path), key_file=str(key_path))
    assert "alice:secret@db.example" in settings.database_url


def test_missing_key_file_raises_clear_error(tmp_path: Path) -> None:
    """A present config but missing key yields a clear ``ConfigError``."""
    config_path, key_path = _write_encrypted_ini(tmp_path)
    key_path.unlink()
    with pytest.raises(ConfigError, match="key file"):
        load_encrypted_database_url(config_path, key_path)


def test_require_database_url_when_unset() -> None:
    """``require_database_url`` raises a clear error when nothing is configured."""
    settings = Settings(database_url="")
    with pytest.raises(ConfigError, match="No database URL"):
        settings.require_database_url()
