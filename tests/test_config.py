"""Configuration-layer tests for the four-source priority chain.

Exercises init-kwarg (CLI flag) override, ``PDBSEARCH_*`` env, the ``.env`` file,
the Fernet-encrypted INI source round-trip, and the lost-key error path. No
developer-local database or credentials are involved.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from paper_sorts.config import ConfigError, Settings


def _write_encrypted_ini(tmp_path: Path, body: str) -> tuple[Path, Path]:
    """Write a Fernet-encrypted INI file and its key, returning both paths.

    :param tmp_path: a temporary directory.
    :param body: the plaintext INI content.
    :return: ``(config_path, key_path)``.
    """
    key = Fernet.generate_key()
    config_path = tmp_path / "database.crypt"
    key_path = tmp_path / "key"
    config_path.write_bytes(Fernet(key).encrypt(body.encode("utf-8")))
    key_path.write_bytes(key)
    return config_path, key_path


def test_init_flag_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    """An init kwarg (CLI flag) overrides the environment variable."""
    monkeypatch.setenv("PDBSEARCH_DATABASE_URL", "postgresql+psycopg://env/db")
    settings = Settings(database_url="postgresql+psycopg://flag/db")
    assert settings.database_url == "postgresql+psycopg://flag/db"


def test_env_var_resolves(monkeypatch: pytest.MonkeyPatch) -> None:
    """A ``PDBSEARCH_*`` environment variable is picked up."""
    monkeypatch.setenv("PDBSEARCH_DATABASE_URL", "postgresql+psycopg://env/db")
    assert Settings().database_url == "postgresql+psycopg://env/db"


def test_dotenv_resolves(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A ``.env`` file is read when no higher-priority source provides the value."""
    monkeypatch.delenv("PDBSEARCH_DATABASE_URL", raising=False)
    (tmp_path / ".env").write_text(
        "PDBSEARCH_DATABASE_URL=postgresql+psycopg://dotenv/db\n", encoding="utf-8"
    )
    monkeypatch.chdir(tmp_path)
    assert Settings().database_url == "postgresql+psycopg://dotenv/db"


def test_env_beats_dotenv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The environment variable wins over the ``.env`` file."""
    (tmp_path / ".env").write_text(
        "PDBSEARCH_DATABASE_URL=postgresql+psycopg://dotenv/db\n", encoding="utf-8"
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PDBSEARCH_DATABASE_URL", "postgresql+psycopg://env/db")
    assert Settings().database_url == "postgresql+psycopg://env/db"


def test_encrypted_ini_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The Fernet-encrypted INI source assembles a database URL."""
    monkeypatch.delenv("PDBSEARCH_DATABASE_URL", raising=False)
    monkeypatch.chdir(tmp_path)
    config_path, key_path = _write_encrypted_ini(
        tmp_path,
        "[postgresql]\nhost=localhost\nport=5432\ndbname=papers\nuser=me\npassword=pw\n",
    )
    settings = Settings(config_path=str(config_path), key_path=str(key_path))
    assert settings.database_url == "postgresql+psycopg://me:pw@localhost:5432/papers"


def test_flag_beats_encrypted_ini(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An explicit database URL flag beats the encrypted INI source."""
    monkeypatch.delenv("PDBSEARCH_DATABASE_URL", raising=False)
    monkeypatch.chdir(tmp_path)
    config_path, key_path = _write_encrypted_ini(
        tmp_path, "[postgresql]\ndbname=papers\nuser=me\npassword=pw\n"
    )
    settings = Settings(
        database_url="postgresql+psycopg://flag/db",
        config_path=str(config_path),
        key_path=str(key_path),
    )
    assert settings.database_url == "postgresql+psycopg://flag/db"


def test_missing_key_raises_config_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A provided config with a missing key file yields a ConfigError."""
    monkeypatch.delenv("PDBSEARCH_DATABASE_URL", raising=False)
    monkeypatch.chdir(tmp_path)
    config_path, _ = _write_encrypted_ini(tmp_path, "[postgresql]\ndbname=papers\n")
    with pytest.raises(ConfigError):
        Settings(config_path=str(config_path), key_path=str(tmp_path / "nope"))


def test_wrong_key_raises_config_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A wrong key for the encrypted config yields a ConfigError."""
    monkeypatch.delenv("PDBSEARCH_DATABASE_URL", raising=False)
    monkeypatch.chdir(tmp_path)
    config_path, _ = _write_encrypted_ini(tmp_path, "[postgresql]\ndbname=papers\n")
    bad_key = tmp_path / "bad_key"
    bad_key.write_bytes(Fernet.generate_key())
    with pytest.raises(ConfigError):
        Settings(config_path=str(config_path), key_path=str(bad_key))


def test_require_database_url_raises_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    """``require_database_url`` raises a ConfigError when nothing is configured."""
    monkeypatch.delenv("PDBSEARCH_DATABASE_URL", raising=False)
    settings = Settings()
    with pytest.raises(ConfigError):
        settings.require_database_url()
