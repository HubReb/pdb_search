"""Unit tests for paper_sorts configuration module.

Tests Settings loading from env vars, .env file, and FernetConfigSource
error handling. No database needed.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from paper_sorts.config import FernetConfigSource, Settings


class TestSettingsFromEnv:
    """Tests for Settings loading from environment variables."""

    def test_database_url_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """PDBSEARCH_DATABASE_URL is picked up as database_url."""
        monkeypatch.setenv("PDBSEARCH_DATABASE_URL", "postgresql+psycopg://localhost/test")
        settings = Settings()
        assert settings.database_url == "postgresql+psycopg://localhost/test"

    def test_log_level_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """PDBSEARCH_LOG_LEVEL is picked up and uppercased."""
        monkeypatch.setenv("PDBSEARCH_LOG_LEVEL", "debug")
        settings = Settings()
        assert settings.log_level == "DEBUG"

    def test_invalid_log_level_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """An unrecognised log level raises a validation error."""
        from pydantic import ValidationError

        monkeypatch.setenv("PDBSEARCH_LOG_LEVEL", "VERBOSE")
        with pytest.raises(ValidationError):
            Settings()


class TestSettingsFromDotEnv:
    """Tests for Settings loading from a .env file."""

    def test_loads_from_dotenv(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Settings reads PDBSEARCH_DATABASE_URL from a .env file."""
        env_file = tmp_path / ".env"
        env_file.write_text("PDBSEARCH_DATABASE_URL=postgresql+psycopg://dotenv/test\n")

        # Clear any env var that would take priority
        monkeypatch.delenv("PDBSEARCH_DATABASE_URL", raising=False)

        # pydantic-settings reads .env from cwd — change to tmp_path
        monkeypatch.chdir(tmp_path)
        settings = Settings()
        assert settings.database_url == "postgresql+psycopg://dotenv/test"


class TestFernetConfigSource:
    """Tests for FernetConfigSource error handling."""

    def test_missing_config_file_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """FernetConfigSource raises ValueError when config_file does not exist."""
        monkeypatch.setenv("PDBSEARCH_CONFIG", str(tmp_path / "nonexistent.crypt"))
        monkeypatch.setenv("PDBSEARCH_KEY", str(tmp_path / "key"))

        from paper_sorts.config import FernetConfigSource, Settings
        src = FernetConfigSource(Settings)
        with pytest.raises(ValueError, match="not found"):
            src()

    def test_missing_key_file_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """FernetConfigSource raises ValueError when key_file does not exist."""
        config_file = tmp_path / "db.crypt"
        config_file.write_bytes(b"fake")
        monkeypatch.setenv("PDBSEARCH_CONFIG", str(config_file))
        monkeypatch.setenv("PDBSEARCH_KEY", str(tmp_path / "nonexistent_key"))

        src = FernetConfigSource(Settings)
        with pytest.raises(ValueError, match="Key file not found"):
            src()

    def test_wrong_key_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """FernetConfigSource raises ValueError when the key cannot decrypt the file."""
        key = Fernet.generate_key()
        wrong_key = Fernet.generate_key()
        fernet = Fernet(key)
        encrypted = fernet.encrypt(b"[postgresql]\ndbname=test\n")

        config_file = tmp_path / "db.crypt"
        config_file.write_bytes(encrypted)
        key_file = tmp_path / "key"
        key_file.write_bytes(wrong_key)

        monkeypatch.setenv("PDBSEARCH_CONFIG", str(config_file))
        monkeypatch.setenv("PDBSEARCH_KEY", str(key_file))

        src = FernetConfigSource(Settings)
        with pytest.raises(ValueError, match="Failed to decrypt"):
            src()

    def test_valid_config_returns_url(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A valid encrypted config returns the database_url."""
        key = Fernet.generate_key()
        fernet = Fernet(key)
        ini = "[postgresql]\nhost=localhost\nport=5432\ndbname=mydb\nuser=me\npassword=secret\n"
        encrypted = fernet.encrypt(ini.encode())

        config_file = tmp_path / "db.crypt"
        config_file.write_bytes(encrypted)
        key_file = tmp_path / "key"
        key_file.write_bytes(key)

        monkeypatch.setenv("PDBSEARCH_CONFIG", str(config_file))
        monkeypatch.setenv("PDBSEARCH_KEY", str(key_file))
        monkeypatch.delenv("PDBSEARCH_DATABASE_URL", raising=False)

        src = FernetConfigSource(Settings)
        result = src()
        assert "mydb" in result.get("database_url", "")
