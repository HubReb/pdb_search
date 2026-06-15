"""Unit tests for the configuration layer (pure helper coverage).

Covers the four-source priority order, the Fernet-encrypted INI source, and the
clear-error paths on a missing key or missing config file (Edge Cases).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from paper_sorts.config import (
    ConfigurationError,
    load_encrypted_url,
    load_settings,
)

INI = (
    "[postgresql]\n"
    "host = db.example\n"
    "port = 6543\n"
    "dbname = papers\n"
    "user = alice\n"
    "password = secret\n"
)


def _write_encrypted(tmp_path: Path) -> tuple[Path, Path]:
    """Write a Fernet key + encrypted INI, returning (config_path, key_path)."""
    key = Fernet.generate_key()
    key_path = tmp_path / "key"
    key_path.write_bytes(key)
    config_path = tmp_path / "database.crypt"
    config_path.write_bytes(Fernet(key).encrypt(INI.encode("utf-8")))
    return config_path, key_path


def test_cli_override_beats_everything(monkeypatch: pytest.MonkeyPatch) -> None:
    """An explicit database_url (CLI) wins over env."""
    monkeypatch.setenv("PDBSEARCH_DATABASE_URL", "postgresql+psycopg://env/db")
    settings = load_settings(database_url="postgresql+psycopg://cli/db")
    assert settings.database_url == "postgresql+psycopg://cli/db"


def test_env_used_when_no_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    """The PDBSEARCH_ env var is used when no CLI override is given."""
    monkeypatch.setenv("PDBSEARCH_DATABASE_URL", "postgresql+psycopg://env/db")
    settings = load_settings()
    assert settings.database_url == "postgresql+psycopg://env/db"


def test_encrypted_source_builds_url(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """When only config+key are given, the URL is assembled from the INI."""
    monkeypatch.delenv("PDBSEARCH_DATABASE_URL", raising=False)
    config_path, key_path = _write_encrypted(tmp_path)
    settings = load_settings(config=str(config_path), key=str(key_path))
    assert settings.database_url == ("postgresql+psycopg://alice:secret@db.example:6543/papers")


def test_load_encrypted_url_directly(tmp_path: Path) -> None:
    """The Fernet decryption helper builds the expected URL."""
    config_path, key_path = _write_encrypted(tmp_path)
    url = load_encrypted_url(str(config_path), str(key_path))
    assert url == "postgresql+psycopg://alice:secret@db.example:6543/papers"


def test_missing_key_is_clear_error(tmp_path: Path) -> None:
    """A missing key file raises a clear ConfigurationError, not a traceback."""
    config_path, _ = _write_encrypted(tmp_path)
    with pytest.raises(ConfigurationError, match="key file not found"):
        load_encrypted_url(str(config_path), str(tmp_path / "nope"))


def test_missing_config_is_clear_error(tmp_path: Path) -> None:
    """A missing config file raises a clear ConfigurationError."""
    _, key_path = _write_encrypted(tmp_path)
    with pytest.raises(ConfigurationError, match="config file not found"):
        load_encrypted_url(str(tmp_path / "absent.crypt"), str(key_path))


def test_wrong_key_is_clear_error(tmp_path: Path) -> None:
    """A wrong key produces a clear decrypt error."""
    config_path, _ = _write_encrypted(tmp_path)
    wrong_key = tmp_path / "wrong"
    wrong_key.write_bytes(Fernet.generate_key())
    with pytest.raises(ConfigurationError, match="could not decrypt"):
        load_encrypted_url(str(config_path), str(wrong_key))
