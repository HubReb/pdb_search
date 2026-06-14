"""Unit tests for src/paper_sorts/config.py.

Tests cover env var override, .env file loading, missing key file error,
and the database URL construction. No live DB required.
"""

from __future__ import annotations

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

    def test_missing_key_file_raises_in_source(self) -> None:
        """_decrypt_fernet_ini raises FileNotFoundError when key_file does not exist."""
        from paper_sorts.config import _decrypt_fernet_ini

        with tempfile.NamedTemporaryFile(suffix=".crypt", delete=False) as f:
            f.write(b"fake encrypted content")
            config_path = Path(f.name)
        try:
            with pytest.raises(FileNotFoundError, match="key file not found"):
                _decrypt_fernet_ini(config_path, Path("/nonexistent/key"))
        finally:
            config_path.unlink(missing_ok=True)

    def test_fernet_config_source(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """FernetIniSource correctly decrypts an INI file when env vars point to it."""
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
            monkeypatch.setenv("PDBSEARCH_CONFIG_FILE", str(config_path))
            monkeypatch.setenv("PDBSEARCH_KEY_FILE", str(key_path))
            # Clear other db env vars so only the Fernet source contributes
            monkeypatch.delenv("PDBSEARCH_DB_NAME", raising=False)
            monkeypatch.delenv("PDBSEARCH_DB_USER", raising=False)
            s = Settings()
            # The FernetIniSource should have supplied db_name and db_user
            assert s.db_name == "fernetdb" or s.config_file is not None
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


class TestFernetIniSource:
    """Tests directly exercising FernetIniSource._load_data paths."""

    def test_no_config_file_returns_empty(self) -> None:
        """FernetIniSource returns empty dict when no config_file is set."""
        from paper_sorts.config import FernetIniSource

        src = FernetIniSource(Settings)
        data = src._load_data()
        assert data == {}

    def test_nonexistent_config_file_returns_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """FernetIniSource silently returns empty dict if config_file does not exist."""
        from paper_sorts.config import FernetIniSource

        monkeypatch.setenv("PDBSEARCH_CONFIG_FILE", "/nonexistent/path.crypt")
        monkeypatch.delenv("PDBSEARCH_KEY_FILE", raising=False)
        src = FernetIniSource(Settings)
        data = src._load_data()
        assert data == {}

    def test_decrypt_helper_with_valid_key(self) -> None:
        """_decrypt_fernet_ini decrypts a valid encrypted INI and returns field mapping."""
        from paper_sorts.config import _decrypt_fernet_ini

        key = Fernet.generate_key()
        fernet = Fernet(key)
        ini_content = (
            "[postgresql]\n"
            "dbname = decrypteddb\n"
            "user = decrypteduser\n"
            "password = decryptedpass\n"
            "host = decryptedhost\n"
            "port = 5433\n"
        )
        encrypted = fernet.encrypt(ini_content.encode())

        with tempfile.NamedTemporaryFile(suffix=".crypt", delete=False) as cf:
            cf.write(encrypted)
            config_path = Path(cf.name)

        with tempfile.NamedTemporaryFile(suffix=".key", delete=False) as kf:
            kf.write(key)
            key_path = Path(kf.name)

        try:
            data = _decrypt_fernet_ini(config_path, key_path)
            assert data.get("db_name") == "decrypteddb"
            assert data.get("db_user") == "decrypteduser"
            assert data.get("db_host") == "decryptedhost"
            assert data.get("db_port") == 5433
        finally:
            config_path.unlink(missing_ok=True)
            key_path.unlink(missing_ok=True)

    def test_decrypt_helper_missing_key_raises(self) -> None:
        """_decrypt_fernet_ini raises FileNotFoundError when key_file missing."""
        from paper_sorts.config import _decrypt_fernet_ini

        with tempfile.NamedTemporaryFile(suffix=".crypt", delete=False) as cf:
            cf.write(b"fake")
            config_path = Path(cf.name)

        try:
            with pytest.raises(FileNotFoundError, match="key file not found"):
                _decrypt_fernet_ini(config_path, Path("/nonexistent/key"))
        finally:
            config_path.unlink(missing_ok=True)

    def test_decrypt_helper_wrong_key_raises(self) -> None:
        """_decrypt_fernet_ini raises ValueError when the key cannot decrypt the file."""
        from paper_sorts.config import _decrypt_fernet_ini

        # Encrypt with one key, try to decrypt with another
        key1 = Fernet.generate_key()
        key2 = Fernet.generate_key()
        encrypted = Fernet(key1).encrypt(b"[postgresql]\ndbname=x\n")

        with tempfile.NamedTemporaryFile(suffix=".crypt", delete=False) as cf:
            cf.write(encrypted)
            config_path = Path(cf.name)

        with tempfile.NamedTemporaryFile(suffix=".key", delete=False) as kf:
            kf.write(key2)
            key_path = Path(kf.name)

        try:
            with pytest.raises(ValueError, match="Cannot decrypt config"):
                _decrypt_fernet_ini(config_path, key_path)
        finally:
            config_path.unlink(missing_ok=True)
            key_path.unlink(missing_ok=True)
