"""Unit tests for config.py (Settings and FernetIniSettingsSource).

No database required — pure unit tests.
"""

import pathlib
import tempfile

import pytest
from cryptography.fernet import Fernet

from paper_sorts.config import Settings


class TestSettings:
    """Tests for the Settings class."""

    def test_env_var_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """PDBSEARCH_DATABASE_URL environment variable is picked up."""
        monkeypatch.setenv("PDBSEARCH_DATABASE_URL", "postgresql+psycopg://test@localhost/db")
        settings = Settings()
        assert settings.database_url == "postgresql+psycopg://test@localhost/db"

    def test_default_log_level(self) -> None:
        """Default log level is INFO when not overridden."""
        settings = Settings(database_url="postgresql+psycopg://x@y/z")
        assert settings.log_level == "INFO"

    def test_log_level_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """PDBSEARCH_LOG_LEVEL is respected."""
        monkeypatch.setenv("PDBSEARCH_LOG_LEVEL", "DEBUG")
        settings = Settings(database_url="postgresql+psycopg://x@y/z")
        assert settings.log_level == "DEBUG"

    def test_constructor_wins_over_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Constructor value takes priority over environment variable."""
        monkeypatch.setenv("PDBSEARCH_DATABASE_URL", "postgresql+psycopg://env@host/db")
        settings = Settings(database_url="postgresql+psycopg://explicit@host/db")
        assert settings.database_url == "postgresql+psycopg://explicit@host/db"

    def test_fernet_ini_source(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """FernetIniSettingsSource decrypts INI and builds DSN."""
        # Clear any leaked env var from other tests
        monkeypatch.delenv("PDBSEARCH_DATABASE_URL", raising=False)
        # Create a real encrypted INI
        key = Fernet.generate_key()
        config_content = (
            "[postgresql]\n"
            "dbname = mydb\n"
            "user = myuser\n"
            "password = mypass\n"
            "host = myhost\n"
            "port = 5432\n"
        )
        encrypted = Fernet(key).encrypt(config_content.encode())

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = pathlib.Path(tmpdir) / "db.crypt"
            key_path = pathlib.Path(tmpdir) / "key"
            config_path.write_bytes(encrypted)
            key_path.write_bytes(key)

            settings = Settings(fernet_config_path=config_path, fernet_key_path=key_path)
            assert "mydb" in settings.database_url
            assert "myuser" in settings.database_url

    def test_missing_fernet_files_graceful(self) -> None:
        """Settings works without Fernet config — just returns empty database_url."""
        settings = Settings()
        # database_url defaults to "" — no crash
        assert isinstance(settings.database_url, str)

    def test_fernet_wrong_key_graceful(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Wrong Fernet key causes graceful fallback (no crash), database_url stays empty."""
        monkeypatch.delenv("PDBSEARCH_DATABASE_URL", raising=False)
        key1 = Fernet.generate_key()
        key2 = Fernet.generate_key()
        config_content = "[postgresql]\ndbname=db\nuser=u\npassword=p\n"
        encrypted = Fernet(key1).encrypt(config_content.encode())

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = pathlib.Path(tmpdir) / "db.crypt"
            key_path = pathlib.Path(tmpdir) / "key"
            config_path.write_bytes(encrypted)
            key_path.write_bytes(key2)  # wrong key

            settings = Settings(fernet_config_path=config_path, fernet_key_path=key_path)
            # Should not crash; database_url stays empty
            assert settings.database_url == ""
