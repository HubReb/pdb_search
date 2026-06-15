"""Unit tests for the configuration loader and its four-source priority chain."""

from __future__ import annotations

from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from paper_sorts.config import ConfigError, load_settings


@pytest.fixture
def encrypted_config(tmp_path: Path) -> tuple[Path, Path]:
    """Write a Fernet-encrypted INI and its key; return (config_path, key_path)."""
    key = Fernet.generate_key()
    key_path = tmp_path / "key"
    key_path.write_bytes(key)
    ini = b"[postgresql]\nhost=eh\nport=5433\ndbname=edb\nuser=eu\npassword=ep\n"
    cfg_path = tmp_path / "cfg.crypt"
    cfg_path.write_bytes(Fernet(key).encrypt(ini))
    return cfg_path, key_path


def test_default_settings() -> None:
    settings = load_settings()
    assert settings.log_level == "INFO"


def test_init_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PDBSEARCH_DATABASE_URL", raising=False)
    settings = load_settings(database_url="postgresql+psycopg://init/db")
    assert settings.database_url == "postgresql+psycopg://init/db"


def test_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PDBSEARCH_DATABASE_URL", "postgresql+psycopg://env/db")
    settings = load_settings()
    assert settings.database_url == "postgresql+psycopg://env/db"


def test_dotenv_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PDBSEARCH_DATABASE_URL", raising=False)
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text("PDBSEARCH_DATABASE_URL=postgresql+psycopg://dotenv/db\n")
    settings = load_settings()
    assert settings.database_url == "postgresql+psycopg://dotenv/db"


def test_encrypted_ini_source(
    encrypted_config: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("PDBSEARCH_DATABASE_URL", raising=False)
    cfg, key = encrypted_config
    settings = load_settings(config_path=str(cfg), key_path=str(key))
    assert settings.database_url == "postgresql+psycopg://eu:ep@eh:5433/edb"


def test_priority_init_beats_encrypted(
    encrypted_config: tuple[Path, Path],
) -> None:
    cfg, key = encrypted_config
    settings = load_settings(
        database_url="postgresql+psycopg://init/db",
        config_path=str(cfg),
        key_path=str(key),
    )
    assert settings.database_url == "postgresql+psycopg://init/db"


def test_lost_key_actionable_error(encrypted_config: tuple[Path, Path], tmp_path: Path) -> None:
    cfg, _ = encrypted_config
    bad_key = tmp_path / "badkey"
    bad_key.write_bytes(Fernet.generate_key())
    with pytest.raises(ConfigError, match="could not decrypt"):
        load_settings(config_path=str(cfg), key_path=str(bad_key))


def test_missing_config_file_actionable_error(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="file not found"):
        load_settings(config_path=str(tmp_path / "nope.crypt"), key_path=str(tmp_path / "k"))
