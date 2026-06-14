"""Unit tests for paper_sorts.config.Settings.

Tests do not require a real database. They verify:
- PDBSEARCH_DATABASE_URL env var overrides the default
- .env file loading (via monkeypatch)
- Missing Fernet key produces a clear error, not a stack trace
- PDBSEARCH_LOG_LEVEL is parsed correctly
- Doc-currency gate: README.md and CLAUDE.md must not contain legacy tokens
"""

from __future__ import annotations

from pathlib import Path

import pytest

from paper_sorts.config import Settings


class TestSettingsEnvVar:
    """Tests for Settings with environment variable overrides."""

    def test_database_url_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """PDBSEARCH_DATABASE_URL environment variable overrides the default."""
        expected = "postgresql+psycopg://user:pw@localhost:5432/testdb"
        monkeypatch.setenv("PDBSEARCH_DATABASE_URL", expected)
        settings = Settings()
        assert settings.get_database_url() == expected

    def test_log_level_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """PDBSEARCH_LOG_LEVEL environment variable is parsed correctly."""
        monkeypatch.setenv("PDBSEARCH_LOG_LEVEL", "DEBUG")
        settings = Settings()
        assert settings.log_level == "DEBUG"

    def test_default_log_level(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Default log level is INFO when no env var is set."""
        monkeypatch.delenv("PDBSEARCH_LOG_LEVEL", raising=False)
        monkeypatch.delenv("PDBSEARCH_DATABASE_URL", raising=False)
        settings = Settings()
        assert settings.log_level == "INFO"

    def test_init_overrides_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Values passed to __init__ override environment variables."""
        monkeypatch.setenv("PDBSEARCH_DATABASE_URL", "postgresql+psycopg://env/db")
        settings = Settings(database_url="postgresql+psycopg://init/db")  # type: ignore[arg-type]
        assert settings.get_database_url() == "postgresql+psycopg://init/db"


class TestFernetIniSource:
    """Tests for the FernetIniSource config source."""

    def test_missing_config_file_raises_clear_error(self, tmp_path: Path) -> None:
        """A missing config file raises ValueError with a clear message (not a stack trace)."""
        from paper_sorts.config import FernetIniSource

        source = FernetIniSource(
            settings_cls=Settings,
            config_file=str(tmp_path / "nonexistent.crypt"),
            key_file=str(tmp_path / "key"),
        )
        with pytest.raises(ValueError, match="not found"):
            source._read_encrypted()

    def test_missing_key_file_raises_clear_error(self, tmp_path: Path) -> None:
        """A missing key file raises ValueError with a clear message."""
        from paper_sorts.config import FernetIniSource

        config_file = tmp_path / "config.crypt"
        config_file.write_bytes(b"dummy")

        source = FernetIniSource(
            settings_cls=Settings,
            config_file=str(config_file),
            key_file=str(tmp_path / "missing_key"),
        )
        with pytest.raises(ValueError, match="not found"):
            source._read_encrypted()

    def test_invalid_fernet_key_raises_clear_error(self, tmp_path: Path) -> None:
        """An invalid Fernet key raises ValueError with a clear message."""
        from paper_sorts.config import FernetIniSource

        config_file = tmp_path / "config.crypt"
        config_file.write_bytes(b"not_valid_fernet_ciphertext")
        key_file = tmp_path / "key"
        key_file.write_bytes(b"not_a_valid_fernet_key")

        source = FernetIniSource(
            settings_cls=Settings,
            config_file=str(config_file),
            key_file=str(key_file),
        )
        with pytest.raises(ValueError, match="[Ff]ailed|[Dd]ecrypt|[Kk]ey"):
            source._read_encrypted()

    def test_none_config_file_returns_empty(self) -> None:
        """FernetIniSource with None config_file returns empty dict (silently skipped)."""
        from paper_sorts.config import FernetIniSource

        source = FernetIniSource(
            settings_cls=Settings,
            config_file=None,
            key_file=None,
        )
        result = source._read_encrypted()
        assert result == {}


class TestDocCurrencyGate:
    """Mechanical doc-currency gate (constitution Principle I, G3).

    README.md and CLAUDE.md must NOT contain any of the forbidden legacy-stack
    tokens after the legacy flat-layout modules are removed.
    Forbidden tokens: Poetry, psycopg2, UserInteraction, PsycopgDB
    """

    FORBIDDEN_TOKENS = ["Poetry", "psycopg2", "UserInteraction", "PsycopgDB"]
    DOCS_TO_CHECK = ["README.md", "CLAUDE.md"]

    @pytest.mark.parametrize("doc_name", DOCS_TO_CHECK)
    @pytest.mark.parametrize("token", FORBIDDEN_TOKENS)
    def test_doc_does_not_contain_legacy_token(
        self, doc_name: str, token: str
    ) -> None:
        """doc_name must not contain the legacy token (case-sensitive)."""
        doc_path = Path(doc_name)
        if not doc_path.exists():
            pytest.skip(f"{doc_name} not found in working directory")
        content = doc_path.read_text(encoding="utf-8")
        assert token not in content, (
            f"{doc_name} contains forbidden legacy-stack token '{token}'. "
            f"This violates the doc-currency gate (constitution Principle I, G3)."
        )


class TestLoggingConfig:
    """Tests for configure_logging."""

    def test_configure_logging_info(self) -> None:
        """configure_logging with INFO level sets up root logger."""
        import logging

        from paper_sorts.logging_config import configure_logging

        configure_logging("INFO")
        assert logging.root.level == logging.INFO

    def test_configure_logging_with_file(self, tmp_path: Path) -> None:
        """configure_logging with log_file creates FileHandler."""
        import logging

        from paper_sorts.logging_config import configure_logging

        log_file = str(tmp_path / "test.log")
        configure_logging("WARNING", log_file=log_file)
        assert logging.root.level == logging.WARNING


class TestFernetIniSourceCall:
    """Tests for FernetIniSource.__call__ which maps decrypted INI to settings."""

    def test_call_with_database_url_key(self, tmp_path: Path) -> None:
        """__call__ extracts database_url if present in decrypted INI."""
        from cryptography.fernet import Fernet

        from paper_sorts.config import FernetIniSource, Settings

        key = Fernet.generate_key()
        fernet = Fernet(key)
        ini_content = "[postgresql]\ndatabase_url = postgresql+psycopg://test:pw@localhost/db\n"
        encrypted = fernet.encrypt(ini_content.encode())

        config_file = tmp_path / "config.crypt"
        key_file = tmp_path / "key"
        config_file.write_bytes(encrypted)
        key_file.write_bytes(key)

        source = FernetIniSource(
            settings_cls=Settings,
            config_file=str(config_file),
            key_file=str(key_file),
        )
        result = source()
        assert "database_url" in result
        assert "postgresql" in result["database_url"]

    def test_call_with_host_keys(self, tmp_path: Path) -> None:
        """__call__ constructs database_url from host/port/dbname/user/password."""
        from cryptography.fernet import Fernet

        from paper_sorts.config import FernetIniSource, Settings

        key = Fernet.generate_key()
        fernet = Fernet(key)
        ini_content = (
            "[postgresql]\n"
            "host = myhost\n"
            "port = 5432\n"
            "dbname = mydb\n"
            "user = myuser\n"
            "password = mypass\n"
        )
        encrypted = fernet.encrypt(ini_content.encode())

        config_file = tmp_path / "config.crypt"
        key_file = tmp_path / "key"
        config_file.write_bytes(encrypted)
        key_file.write_bytes(key)

        source = FernetIniSource(
            settings_cls=Settings,
            config_file=str(config_file),
            key_file=str(key_file),
        )
        result = source()
        assert "database_url" in result
        assert "myhost" in result["database_url"]
