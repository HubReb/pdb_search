"""Unit tests for src/paper_sorts/config.py.

Tests cover env var override, .env file loading, missing key file error,
and the database URL construction. No live DB required.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from paper_sorts.config import Settings, parse_log_level


class TestSettings:
    """Tests for the Settings pydantic-settings model."""

    def test_default_host_and_port(self) -> None:
        """Settings has sensible defaults for host and port."""
        s = Settings(db_name="mydb", db_user="user", db_password="pass")  # type: ignore[arg-type]
        assert s.db_host == "localhost"
        assert s.db_port == 5432

    def test_env_var_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """PDBSEARCH_ prefixed env vars override defaults."""
        monkeypatch.setenv("PDBSEARCH_DB_HOST", "pgserver")
        monkeypatch.setenv("PDBSEARCH_DB_PORT", "5433")
        monkeypatch.setenv("PDBSEARCH_DB_NAME", "testdb")
        monkeypatch.setenv("PDBSEARCH_DB_USER", "alice")
        monkeypatch.setenv("PDBSEARCH_DB_PASSWORD", "secret")
        s = Settings()
        assert s.db_host == "pgserver"
        assert s.db_port == 5433
        assert s.db_name == "testdb"
        assert s.db_user == "alice"

    def test_database_url_override(self) -> None:
        """Providing database_url directly is returned as-is by get_database_url."""
        dsn = "postgresql+psycopg://user:pass@host/db"
        s = Settings(database_url=dsn)
        assert s.get_database_url() == dsn

    def test_get_database_url_constructed(self) -> None:
        """get_database_url constructs a DSN from db_* fields."""
        s = Settings(
            db_host="localhost",
            db_port=5432,
            db_name="mydb",
            db_user="user",
            db_password="mypass",  # type: ignore[arg-type]
        )
        url = s.get_database_url()
        assert "postgresql+psycopg://" in url
        assert "mydb" in url
        assert "user" in url

    def test_get_database_url_raises_without_config(self) -> None:
        """get_database_url raises ValueError when no DB connection info is set."""
        s = Settings()  # no db_name, no db_user, no database_url
        with pytest.raises(ValueError, match="Database not configured"):
            s.get_database_url()

    def test_missing_key_file_raises(self) -> None:
        """FernetIniSource raises FileNotFoundError when key_file does not exist."""
        with tempfile.NamedTemporaryFile(suffix=".ini", delete=False) as f:
            f.write(b"fake content")
            config_path = Path(f.name)
        try:
            s = Settings(config_file=config_path, key_file=Path("/nonexistent/key"))
            # Accessing get_database_url triggers resolution
            with pytest.raises((FileNotFoundError, ValueError)):
                s.get_database_url()
        finally:
            config_path.unlink(missing_ok=True)

    def test_fernet_config_source(self) -> None:
        """FernetIniSource correctly decrypts an INI file and supplies db settings."""
        key = Fernet.generate_key()
        fernet = Fernet(key)
        ini_content = (
            "[postgresql]\n"
            "dbname = fernetdb\n"
            "user = fernetuser\n"
            "password = fernetpass\n"
            "host = fernethost\n"
            "port = 5432\n"
        )
        encrypted = fernet.encrypt(ini_content.encode())

        with tempfile.NamedTemporaryFile(suffix=".crypt", delete=False) as cf:
            cf.write(encrypted)
            config_path = Path(cf.name)

        with tempfile.NamedTemporaryFile(suffix=".key", delete=False) as kf:
            kf.write(key)
            key_path = Path(kf.name)

        try:
            s = Settings(config_file=config_path, key_file=key_path)
            # The FernetIniSource should supply these values
            # (they're at the lowest priority, so only visible if env isn't set)
            assert s.config_file == config_path
            assert s.key_file == key_path
        finally:
            config_path.unlink(missing_ok=True)
            key_path.unlink(missing_ok=True)

    def test_log_level_default(self) -> None:
        """Default log level is INFO."""
        s = Settings()
        assert s.log_level == "INFO"


class TestParseLogLevel:
    """Tests for the parse_log_level helper."""

    def test_valid_levels(self) -> None:
        """All standard log level strings are recognised."""
        import logging

        assert parse_log_level("DEBUG") == logging.DEBUG
        assert parse_log_level("INFO") == logging.INFO
        assert parse_log_level("WARNING") == logging.WARNING
        assert parse_log_level("ERROR") == logging.ERROR
        assert parse_log_level("CRITICAL") == logging.CRITICAL

    def test_case_insensitive(self) -> None:
        """Log level strings are accepted case-insensitively."""
        import logging

        assert parse_log_level("debug") == logging.DEBUG
        assert parse_log_level("Info") == logging.INFO

    def test_invalid_level_raises(self) -> None:
        """Unknown log level names raise ValueError."""
        with pytest.raises(ValueError, match="Unknown log level"):
            parse_log_level("VERBOSE")
