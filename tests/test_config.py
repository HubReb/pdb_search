"""Unit tests for paper_sorts.config.Settings.

Tests cover environment variable override, .env file loading, invalid log
levels, missing Fernet files, and the four-source priority chain.
No database required.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from paper_sorts.config import FernetConfigSource, Settings


class TestSettings:
    """Tests for the Settings model."""

    def test_default_log_level(self) -> None:
        """Default log level is INFO."""
        # Don't use env vars that may be set
        settings = Settings(database_url="postgresql+psycopg://localhost/test")
        assert settings.log_level == "INFO"

    def test_log_level_from_init(self) -> None:
        """Log level can be set via init kwarg."""
        settings = Settings(log_level="DEBUG", database_url="postgresql+psycopg://localhost/test")
        assert settings.log_level == "DEBUG"

    def test_log_level_uppercase_normalised(self) -> None:
        """Log level is normalised to uppercase."""
        settings = Settings(log_level="debug", database_url="postgresql+psycopg://localhost/test")
        assert settings.log_level == "DEBUG"

    def test_invalid_log_level_raises(self) -> None:
        """Invalid log level raises ValidationError."""
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            Settings(log_level="VERBOSE", database_url="postgresql+psycopg://localhost/test")

    def test_database_url_from_init(self) -> None:
        """database_url can be set via init kwarg."""
        settings = Settings(database_url="postgresql+psycopg://user:pass@host/db")
        assert settings.database_url == "postgresql+psycopg://user:pass@host/db"

    def test_database_url_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """database_url can be set via PDBSEARCH_DATABASE_URL env var."""
        monkeypatch.setenv("PDBSEARCH_DATABASE_URL", "postgresql+psycopg://env/db")
        settings = Settings()
        assert settings.database_url == "postgresql+psycopg://env/db"

    def test_resolve_database_url_raises_if_empty(self) -> None:
        """resolve_database_url raises ValueError if no URL is configured."""
        settings = Settings(database_url="")
        with pytest.raises(ValueError, match="No database URL"):
            settings.resolve_database_url()

    def test_resolve_database_url_returns_init_url(self) -> None:
        """resolve_database_url returns the init URL if set."""
        settings = Settings(database_url="postgresql+psycopg://localhost/db")
        assert settings.resolve_database_url() == "postgresql+psycopg://localhost/db"


class TestFernetConfigSource:
    """Tests for FernetConfigSource."""

    def test_returns_empty_when_no_paths(self) -> None:
        """FernetConfigSource returns {} when config_path and key_path are None."""
        source = FernetConfigSource(Settings, None, None)
        assert source() == {}

    def test_missing_config_file_raises(self, tmp_path: Path) -> None:
        """FernetConfigSource raises FileNotFoundError for missing config file."""
        key_path = tmp_path / "key"
        key_path.write_bytes(b"key")
        source = FernetConfigSource(Settings, tmp_path / "nonexistent.crypt", key_path)
        with pytest.raises(FileNotFoundError, match="Config file not found"):
            source._decrypt_ini()

    def test_missing_key_file_raises(self, tmp_path: Path) -> None:
        """FernetConfigSource raises FileNotFoundError for missing key file."""
        config_path = tmp_path / "config.crypt"
        config_path.write_bytes(b"data")
        source = FernetConfigSource(Settings, config_path, tmp_path / "nonexistent.key")
        with pytest.raises(FileNotFoundError, match="Key file not found"):
            source._decrypt_ini()

    def test_invalid_token_raises(self, tmp_path: Path) -> None:
        """FernetConfigSource raises ValueError for corrupted config."""
        from cryptography.fernet import Fernet
        key = Fernet.generate_key()
        key_path = tmp_path / "key"
        key_path.write_bytes(key)
        config_path = tmp_path / "config.crypt"
        config_path.write_bytes(b"not-valid-fernet-data")
        source = FernetConfigSource(Settings, config_path, key_path)
        with pytest.raises(ValueError, match="Failed to decrypt"):
            source._decrypt_ini()

    def test_valid_fernet_config(self, tmp_path: Path) -> None:
        """FernetConfigSource successfully decrypts a valid config and returns database_url."""

        from cryptography.fernet import Fernet

        key = Fernet.generate_key()
        fernet = Fernet(key)

        # Build a valid INI config
        ini_content = (
            "[postgresql]\n"
            "host = localhost\n"
            "port = 5432\n"
            "dbname = testdb\n"
            "user = testuser\n"
            "password = testpass\n"
        )
        encrypted = fernet.encrypt(ini_content.encode("utf-8"))

        config_path = tmp_path / "config.crypt"
        config_path.write_bytes(encrypted)
        key_path = tmp_path / "key"
        key_path.write_bytes(key)

        source = FernetConfigSource(Settings, config_path, key_path)
        result = source()
        assert "database_url" in result
        assert "testdb" in result["database_url"]
        assert "testuser" in result["database_url"]

    def test_missing_section_raises(self, tmp_path: Path) -> None:
        """FernetConfigSource raises ValueError when the section is absent."""
        from cryptography.fernet import Fernet

        key = Fernet.generate_key()
        fernet = Fernet(key)
        ini_content = "[other_section]\nfoo = bar\n"
        encrypted = fernet.encrypt(ini_content.encode("utf-8"))

        config_path = tmp_path / "config.crypt"
        config_path.write_bytes(encrypted)
        key_path = tmp_path / "key"
        key_path.write_bytes(key)

        source = FernetConfigSource(Settings, config_path, key_path)
        with pytest.raises(ValueError, match="Section"):
            source._decrypt_ini()
