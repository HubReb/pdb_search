"""Unit tests for paper_sorts.config.Settings."""

from __future__ import annotations

import tempfile

import pytest

from paper_sorts.config import Settings


class TestSettings:
    """Tests for Settings construction and validation."""

    def test_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Settings without any input has empty database_url and INFO log level."""
        monkeypatch.delenv("PDBSEARCH_DATABASE_URL", raising=False)
        monkeypatch.delenv("PDBSEARCH_LOG_LEVEL", raising=False)
        s = Settings()
        assert s.database_url == ""
        assert s.log_level == "INFO"

    def test_env_var_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """PDBSEARCH_DATABASE_URL env var sets database_url."""
        monkeypatch.setenv("PDBSEARCH_DATABASE_URL", "postgresql+psycopg://u:p@h/db")
        s = Settings()
        assert s.database_url == "postgresql+psycopg://u:p@h/db"

    def test_init_kwarg_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Init kwargs take highest priority over env vars."""
        monkeypatch.setenv("PDBSEARCH_DATABASE_URL", "postgresql+psycopg://env/db")
        s = Settings(database_url="postgresql+psycopg://init/db")
        assert s.database_url == "postgresql+psycopg://init/db"

    def test_log_level_uppercase(self) -> None:
        """log_level is stored in uppercase."""
        s = Settings(log_level="debug")
        assert s.log_level == "DEBUG"

    def test_invalid_log_level_raises(self) -> None:
        """An invalid log level raises ValidationError."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            Settings(log_level="VERBOSE")

    def test_missing_key_file_logs_warning(
        self, tmp_path: object, caplog: pytest.LogCaptureFixture
    ) -> None:
        """FernetSettingsSource logs a warning when key file is missing and returns empty dict."""
        import logging

        cfg = tempfile.NamedTemporaryFile(suffix=".crypt", delete=False)
        cfg.write(b"dummy")
        cfg.close()

        from paper_sorts.config import FernetSettingsSource

        with caplog.at_level(logging.WARNING, logger="paper_sorts.config"):
            src = FernetSettingsSource(
                Settings,  # type: ignore[arg-type]
                config_file=cfg.name,
                key_file="/nonexistent/key/file",
            )
            result = src()

        assert result == {}
        assert any("Key file not found" in r.message for r in caplog.records)

    def test_dot_env_file(self, tmp_path: object, monkeypatch: pytest.MonkeyPatch) -> None:
        """.env file in working directory is read by Settings."""

        env_file = str(tmp_path) + "/.env"  # type: ignore[operator]
        with open(env_file, "w") as f:
            f.write("PDBSEARCH_DATABASE_URL=postgresql+psycopg://dotenv/db\n")

        monkeypatch.chdir(tmp_path)  # type: ignore[arg-type]
        monkeypatch.delenv("PDBSEARCH_DATABASE_URL", raising=False)
        s = Settings()
        assert s.database_url == "postgresql+psycopg://dotenv/db"
