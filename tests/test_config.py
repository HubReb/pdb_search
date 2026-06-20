"""Unit tests for the four-source settings chain and the Fernet source."""

from __future__ import annotations

from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from paper_sorts.config import ConfigError, decrypt_ini, load_settings


def _write_encrypted(tmp_path: Path) -> tuple[Path, Path]:
    key = Fernet.generate_key()
    ini = "[postgresql]\nhost=localhost\nport=5432\ndbname=mydb\nuser=alice\npassword=secret\n"
    token = Fernet(key).encrypt(ini.encode("utf-8"))
    config_path = tmp_path / "database.crypt"
    key_path = tmp_path / "key"
    config_path.write_bytes(token)
    key_path.write_bytes(key)
    return config_path, key_path


def test_decrypt_ini_roundtrip(tmp_path: Path) -> None:
    config_path, key_path = _write_encrypted(tmp_path)
    values = decrypt_ini(config_path, key_path)
    assert values["dbname"] == "mydb"
    assert values["user"] == "alice"


def test_fernet_source_builds_url(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PDBSEARCH_DATABASE_URL", raising=False)
    config_path, key_path = _write_encrypted(tmp_path)
    settings = load_settings(config_path=config_path, key_path=key_path)
    assert settings.database_url == "postgresql+psycopg://alice:secret@localhost:5432/mydb"


def test_cli_overrides_fernet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PDBSEARCH_DATABASE_URL", raising=False)
    config_path, key_path = _write_encrypted(tmp_path)
    settings = load_settings(
        database_url="postgresql+psycopg://override/db",
        config_path=config_path,
        key_path=key_path,
    )
    assert settings.database_url == "postgresql+psycopg://override/db"


def test_env_used_when_no_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PDBSEARCH_DATABASE_URL", "postgresql+psycopg://envhost/db")
    settings = load_settings()
    assert settings.database_url == "postgresql+psycopg://envhost/db"


def test_lost_key_clear_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PDBSEARCH_DATABASE_URL", raising=False)
    config_path, _ = _write_encrypted(tmp_path)
    wrong_key = tmp_path / "wrong_key"
    wrong_key.write_bytes(Fernet.generate_key())
    with pytest.raises(ConfigError) as exc:
        load_settings(config_path=config_path, key_path=wrong_key)
    assert "key" in str(exc.value).lower()


def test_missing_config_file_clear_error(tmp_path: Path) -> None:
    with pytest.raises(ConfigError):
        decrypt_ini(tmp_path / "nope.crypt", tmp_path / "nope.key")


def test_missing_section_clear_error(tmp_path: Path) -> None:
    key = Fernet.generate_key()
    token = Fernet(key).encrypt(b"[other]\nx=1\n")
    config_path = tmp_path / "c.crypt"
    key_path = tmp_path / "k"
    config_path.write_bytes(token)
    key_path.write_bytes(key)
    with pytest.raises(ConfigError):
        decrypt_ini(config_path, key_path, section="postgresql")
