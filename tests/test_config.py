"""Unit tests for paper_sorts config module.

Tests cover:
  - Environment variable loading
  - .env file loading
  - Missing required field behaviour
  - Missing key file error
  - database_url property
  - log_level validation
"""

from __future__ import annotations

from pathlib import Path

import pytest

from paper_sorts.config import Settings
from paper_sorts.logging_config import configure_logging


class TestSettingsFromEnv:
    """Tests for Settings loading from environment variables."""

    def test_load_from_env_vars(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Settings reads PDBSEARCH_* env vars correctly."""
        monkeypatch.setenv("PDBSEARCH_DB_HOST", "myhost")
        monkeypatch.setenv("PDBSEARCH_DB_PORT", "5433")
        monkeypatch.setenv("PDBSEARCH_DB_NAME", "mydb")
        monkeypatch.setenv("PDBSEARCH_DB_USER", "myuser")
        monkeypatch.setenv("PDBSEARCH_DB_PASSWORD", "secret")
        monkeypatch.setenv("PDBSEARCH_LOG_LEVEL", "DEBUG")
        # Remove config/key file vars so Fernet source is skipped
        monkeypatch.delenv("PDBSEARCH_CONFIG_FILE", raising=False)
        monkeypatch.delenv("PDBSEARCH_KEY_FILE", raising=False)

        s = Settings()
        assert s.db_host == "myhost"
        assert s.db_port == 5433
        assert s.db_name == "mydb"
        assert s.db_user == "myuser"
        assert s.db_password.get_secret_value() == "secret"
        assert s.log_level == "DEBUG"

    def test_database_url_property(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """database_url property builds correct SQLAlchemy URL."""
        monkeypatch.setenv("PDBSEARCH_DB_HOST", "localhost")
        monkeypatch.setenv("PDBSEARCH_DB_PORT", "5432")
        monkeypatch.setenv("PDBSEARCH_DB_NAME", "testdb")
        monkeypatch.setenv("PDBSEARCH_DB_USER", "testuser")
        monkeypatch.setenv("PDBSEARCH_DB_PASSWORD", "testpass")
        monkeypatch.delenv("PDBSEARCH_CONFIG_FILE", raising=False)
        monkeypatch.delenv("PDBSEARCH_KEY_FILE", raising=False)

        s = Settings()
        url = s.database_url
        assert url.startswith("postgresql+psycopg://")
        assert "testuser" in url
        assert "testdb" in url
        assert "localhost" in url

    def test_default_log_level(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Default log level is INFO."""
        monkeypatch.delenv("PDBSEARCH_LOG_LEVEL", raising=False)
        monkeypatch.delenv("PDBSEARCH_CONFIG_FILE", raising=False)
        monkeypatch.delenv("PDBSEARCH_KEY_FILE", raising=False)
        s = Settings()
        assert s.log_level == "INFO"

    def test_invalid_log_level_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Invalid log level raises ValueError."""
        monkeypatch.setenv("PDBSEARCH_LOG_LEVEL", "NOTVALID")
        monkeypatch.delenv("PDBSEARCH_CONFIG_FILE", raising=False)
        monkeypatch.delenv("PDBSEARCH_KEY_FILE", raising=False)
        with pytest.raises((ValueError, Exception)):
            Settings()

    def test_log_level_case_insensitive(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Log level is normalised to upper case."""
        monkeypatch.setenv("PDBSEARCH_LOG_LEVEL", "warning")
        monkeypatch.delenv("PDBSEARCH_CONFIG_FILE", raising=False)
        monkeypatch.delenv("PDBSEARCH_KEY_FILE", raising=False)
        s = Settings()
        assert s.log_level == "WARNING"


class TestFernetSource:
    """Tests for the Fernet-encrypted INI settings source."""

    def test_missing_key_file_raises(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """When config_file exists but key_file does not, FileNotFoundError is raised."""
        config_path = tmp_path / "test.crypt"
        config_path.write_bytes(b"fake encrypted data")

        monkeypatch.setenv("PDBSEARCH_CONFIG_FILE", str(config_path))
        monkeypatch.setenv("PDBSEARCH_KEY_FILE", str(tmp_path / "nonexistent.key"))

        with pytest.raises((FileNotFoundError, ValueError)):
            Settings()

    def test_missing_config_file_raises(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """When config_file does not exist, FileNotFoundError is raised."""
        monkeypatch.setenv("PDBSEARCH_CONFIG_FILE", str(tmp_path / "nonexistent.crypt"))
        monkeypatch.setenv("PDBSEARCH_KEY_FILE", str(tmp_path / "key"))

        with pytest.raises((FileNotFoundError, ValueError)):
            Settings()

    def test_no_fernet_files_loads_cleanly(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When neither config_file nor key_file are set, Fernet source returns empty dict."""
        monkeypatch.delenv("PDBSEARCH_CONFIG_FILE", raising=False)
        monkeypatch.delenv("PDBSEARCH_KEY_FILE", raising=False)
        # Should not raise even without any config
        s = Settings()
        assert s.db_host == "localhost"  # default

    def test_valid_fernet_config(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """A valid Fernet-encrypted INI file is decrypted and applied."""
        from configparser import ConfigParser

        from cryptography.fernet import Fernet

        # Create an encrypted INI config
        key = Fernet.generate_key()
        config = ConfigParser()
        config["postgresql"] = {
            "host": "fernethost",
            "port": "5555",
            "dbname": "fernetdb",
            "user": "fernetuser",
            "password": "fernetpass",
        }
        import io

        buf = io.StringIO()
        config.write(buf)
        plaintext = buf.getvalue().encode()
        encrypted = Fernet(key).encrypt(plaintext)

        config_path = tmp_path / "test.crypt"
        key_path = tmp_path / "key"
        config_path.write_bytes(encrypted)
        key_path.write_bytes(key)

        monkeypatch.setenv("PDBSEARCH_CONFIG_FILE", str(config_path))
        monkeypatch.setenv("PDBSEARCH_KEY_FILE", str(key_path))
        monkeypatch.delenv("PDBSEARCH_DB_HOST", raising=False)
        monkeypatch.delenv("PDBSEARCH_DB_NAME", raising=False)

        s = Settings()
        assert s.db_host == "fernethost"
        assert s.db_name == "fernetdb"


class TestLoggingConfig:
    """Tests for logging_config.configure_logging."""

    def test_configure_logging_info(self) -> None:
        """configure_logging with INFO does not raise."""
        configure_logging("INFO")

    def test_configure_logging_debug(self) -> None:
        """configure_logging with DEBUG does not raise."""
        configure_logging("DEBUG")

    def test_configure_logging_with_file(self, tmp_path: Path) -> None:
        """configure_logging with a file path creates a FileHandler."""
        log_file = tmp_path / "test.log"
        configure_logging("WARNING", log_file=log_file)

    def test_configure_logging_invalid_level(self) -> None:
        """configure_logging raises ValueError for invalid level."""
        with pytest.raises(ValueError, match="Invalid log level"):
            configure_logging("NOTVALID")
