"""Unit tests for the configuration layer.

Covers the four-source priority chain and the Fernet-encrypted INI source, including the clear
error when the key file is missing.
"""

from __future__ import annotations

from configparser import ConfigParser
from io import StringIO
from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from paper_sorts.config import load_settings


def _write_encrypted_ini(tmp_path: Path) -> tuple[Path, Path]:
    """Write a Fernet-encrypted INI and its key to ``tmp_path``.

    :param tmp_path: the pytest temporary directory.
    :return: ``(config_path, key_path)``.
    """
    parser = ConfigParser()
    parser["postgresql"] = {
        "dbname": "mydb",
        "user": "alice",
        "password": "secret",
        "host": "db.example",
        "port": "5433",
    }
    buf = StringIO()
    parser.write(buf)
    key = Fernet.generate_key()
    token = Fernet(key).encrypt(buf.getvalue().encode("utf-8"))
    config_path = tmp_path / "database.crypt"
    key_path = tmp_path / "key"
    config_path.write_bytes(token)
    key_path.write_bytes(key)
    return config_path, key_path


def test_cli_flag_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    """An explicit database_url overrides everything else."""
    monkeypatch.setenv("PDBSEARCH_DATABASE_URL", "postgresql+psycopg://env/db")
    settings = load_settings(database_url="postgresql+psycopg://flag/db")
    assert settings.database_url == "postgresql+psycopg://flag/db"


def test_env_var_used(monkeypatch: pytest.MonkeyPatch) -> None:
    """PDBSEARCH_DATABASE_URL is read when no CLI flag is given."""
    monkeypatch.setenv("PDBSEARCH_DATABASE_URL", "postgresql+psycopg://env/db")
    settings = load_settings()
    assert settings.database_url == "postgresql+psycopg://env/db"


def test_fernet_ini_fallback(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The encrypted INI is used when no higher-priority source provides a URL."""
    monkeypatch.delenv("PDBSEARCH_DATABASE_URL", raising=False)
    config_path, key_path = _write_encrypted_ini(tmp_path)
    settings = load_settings(config_path=config_path, key_path=key_path)
    assert settings.database_url == "postgresql+psycopg://alice:secret@db.example:5433/mydb"


def test_missing_key_file_clear_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A missing key file produces a clear FileNotFoundError, not a stack trace."""
    monkeypatch.delenv("PDBSEARCH_DATABASE_URL", raising=False)
    config_path, key_path = _write_encrypted_ini(tmp_path)
    key_path.unlink()
    with pytest.raises(FileNotFoundError) as exc:
        load_settings(config_path=config_path, key_path=key_path)
    assert "key file" in str(exc.value).lower()
