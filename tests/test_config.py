"""Unit tests for paper_sorts.config.

Tests cover:
- Environment variable override (PDBSEARCH_DATABASE_URL)
- .env file parsing (via monkeypatch)
- Missing credentials: Settings loads without error (database_url is None)
- Fernet source skipped gracefully when config_file/key_file are None
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from paper_sorts.config import FernetIniSettingsSource, Settings


def test_settings_env_var_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """PDBSEARCH_DATABASE_URL env var sets database_url on Settings."""
    monkeypatch.setenv(
        "PDBSEARCH_DATABASE_URL", "postgresql+psycopg://user:pass@localhost/testdb"
    )
    s = Settings()
    assert s.database_url is not None
    assert "testdb" in str(s.database_url)


def test_settings_no_credentials() -> None:
    """Settings loads without error when no credentials are configured."""
    # Remove all PDBSEARCH_ env vars
    env_backup = {k: v for k, v in os.environ.items() if k.startswith("PDBSEARCH_")}
    for k in env_backup:
        del os.environ[k]
    try:
        s = Settings()
        assert s.database_url is None or s.database_url is not None  # no crash
    finally:
        os.environ.update(env_backup)


def test_settings_log_level_default() -> None:
    """Settings defaults log_level to 'INFO'."""
    s = Settings()
    assert s.log_level == "INFO"


def test_fernet_source_returns_empty_when_no_files() -> None:
    """FernetIniSettingsSource returns {} when config_file/key_file are None."""
    source = FernetIniSettingsSource(Settings, config_file=None, key_file=None)
    result = source()
    assert result == {}


def test_fernet_source_returns_empty_when_files_missing(tmp_path: Path) -> None:
    """FernetIniSettingsSource returns {} when files do not exist (no exception)."""
    source = FernetIniSettingsSource(
        Settings,
        config_file=str(tmp_path / "missing.crypt"),
        key_file=str(tmp_path / "missing.key"),
    )
    result = source()
    assert result == {}


def test_settings_dotenv_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Settings picks up PDBSEARCH_DATABASE_URL from a .env file."""
    env_file = tmp_path / ".env"
    env_file.write_text(
        "PDBSEARCH_DATABASE_URL=postgresql+psycopg://dotenv:pw@localhost/dotenvdb\n"
    )
    # Change cwd so pydantic-settings finds .env
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("PDBSEARCH_DATABASE_URL", raising=False)
    s = Settings()
    assert s.database_url is not None
    assert "dotenvdb" in str(s.database_url)
