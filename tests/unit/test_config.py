"""Unit tests for ``paper_sorts.config`` (T029)."""

from __future__ import annotations

import configparser
import io
from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from paper_sorts.config import Settings


def _write_fernet_ini(
    tmp_path: Path, postgres_section: dict[str, str]
) -> tuple[Path, Path]:
    """Generate a Fernet-encrypted INI + key file; return their paths."""
    cp = configparser.ConfigParser()
    cp["postgresql"] = postgres_section
    sink = io.StringIO()
    cp.write(sink)
    plaintext = sink.getvalue().encode()
    key = Fernet.generate_key()
    encrypted = Fernet(key).encrypt(plaintext)
    config_path = tmp_path / "db.crypt"
    config_path.write_bytes(encrypted)
    key_path = tmp_path / "key"
    key_path.write_bytes(key)
    return config_path, key_path


@pytest.fixture
def isolated_cwd(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Run the test in tmp_path with no PDBSEARCH_* env vars set."""
    monkeypatch.chdir(tmp_path)
    for var in (
        "PDBSEARCH_DATABASE_URL",
        "PDBSEARCH_LOG_LEVEL",
        "PDBSEARCH_LOG_FILE",
        "PDBSEARCH_FERNET_CONFIG",
        "PDBSEARCH_FERNET_KEY",
    ):
        monkeypatch.delenv(var, raising=False)
    return tmp_path


def test_init_kwarg_wins_over_env_and_fernet(
    monkeypatch: pytest.MonkeyPatch, isolated_cwd: Path
) -> None:
    monkeypatch.setenv(
        "PDBSEARCH_DATABASE_URL", "postgresql+psycopg://env@h/env_db"
    )
    config_path, key_path = _write_fernet_ini(
        isolated_cwd,
        {"dbname": "fernet_db", "user": "fuser", "password": "fpw"},
    )
    s = Settings(
        database_url="postgresql+psycopg://kwarg@h/kwarg_db",
        fernet_config=config_path,
        fernet_key=key_path,
    )
    assert s.database_url == "postgresql+psycopg://kwarg@h/kwarg_db"


def test_env_wins_over_dotenv_and_fernet(
    monkeypatch: pytest.MonkeyPatch, isolated_cwd: Path
) -> None:
    monkeypatch.setenv(
        "PDBSEARCH_DATABASE_URL", "postgresql+psycopg://env@h/env_db"
    )
    (isolated_cwd / ".env").write_text(
        "PDBSEARCH_DATABASE_URL=postgresql+psycopg://dot@h/dot_db\n"
    )
    config_path, key_path = _write_fernet_ini(
        isolated_cwd,
        {"dbname": "fernet_db", "user": "fuser", "password": "fpw"},
    )
    s = Settings(fernet_config=config_path, fernet_key=key_path)
    assert s.database_url == "postgresql+psycopg://env@h/env_db"


def test_dotenv_wins_over_fernet(isolated_cwd: Path) -> None:
    (isolated_cwd / ".env").write_text(
        "PDBSEARCH_DATABASE_URL=postgresql+psycopg://dot@h/dot_db\n"
    )
    config_path, key_path = _write_fernet_ini(
        isolated_cwd,
        {"dbname": "fernet_db", "user": "fuser", "password": "fpw"},
    )
    s = Settings(fernet_config=config_path, fernet_key=key_path)
    assert s.database_url == "postgresql+psycopg://dot@h/dot_db"


def test_fernet_only_assembles_url_with_quoted_credentials(
    isolated_cwd: Path,
) -> None:
    config_path, key_path = _write_fernet_ini(
        isolated_cwd,
        {
            "dbname": "fdb",
            "user": "fuser",
            "password": "p@ss/word",
            "host": "h.example",
            "port": "5433",
        },
    )
    s = Settings(fernet_config=config_path, fernet_key=key_path)
    assert (
        s.database_url
        == "postgresql+psycopg://fuser:p%40ss%2Fword@h.example:5433/fdb"
    )


def test_fernet_config_without_key_raises(isolated_cwd: Path) -> None:
    """Spec edge case "lost key": fernet_config set, fernet_key missing."""
    with pytest.raises(ValueError, match="Fernet config requires a key file"):
        Settings(fernet_config=isolated_cwd / "nonexistent.crypt", fernet_key=None)


def test_missing_database_url_raises(isolated_cwd: Path) -> None:
    """All four sources empty -> the documented ValueError fires."""
    with pytest.raises(ValueError, match="database_url must be set"):
        Settings()


def test_fernet_with_wrong_key_raises_clear_error(
    isolated_cwd: Path,
) -> None:
    config_path, _good_key = _write_fernet_ini(
        isolated_cwd,
        {"dbname": "fdb", "user": "u", "password": "p"},
    )
    bad_key_path = isolated_cwd / "bad_key"
    bad_key_path.write_bytes(Fernet.generate_key())
    with pytest.raises(
        ValueError, match="could not be decrypted with the given key file"
    ):
        Settings(fernet_config=config_path, fernet_key=bad_key_path)
