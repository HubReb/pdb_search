"""Application configuration via pydantic-settings.

Settings resolve the database URL and log level from four sources, highest priority first:

1. CLI flags (passed explicitly to :func:`load_settings`),
2. environment variables prefixed ``PDBSEARCH_``,
3. a ``.env`` file in the working directory,
4. a Fernet-encrypted INI file (the legacy ``database.crypt`` workflow).

Secrets (passwords, decrypted config, keys) are never written to logs.
"""

from __future__ import annotations

from configparser import ConfigParser
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet
from pydantic_settings import BaseSettings, SettingsConfigDict


def _decrypt_ini(config_path: Path, key_path: Path, section: str) -> dict[str, str]:
    """Decrypt a Fernet-encrypted INI file and return one section as a dict.

    :param config_path: path to the Fernet-encrypted INI file.
    :param key_path: path to the Fernet key file.
    :param section: the INI section to read (e.g. ``"postgresql"``).
    :return: the section's key/value pairs.
    :raises FileNotFoundError: if the config or key file is missing (clear, actionable error).
    :raises ValueError: if the section is absent from the decrypted file.
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Encrypted config file not found: {config_path}")
    if not key_path.exists():
        raise FileNotFoundError(
            f"Decryption key file not found: {key_path} (cannot read {config_path})"
        )
    key = key_path.read_bytes()
    payload = config_path.read_bytes()
    decrypted = Fernet(key).decrypt(payload).decode("utf-8")
    parser = ConfigParser()
    parser.read_string(decrypted)
    if not parser.has_section(section):
        raise ValueError(f"Section {section} not found in {config_path}")
    return dict(parser.items(section))


def _url_from_ini(values: dict[str, str]) -> str:
    """Build a SQLAlchemy URL from legacy INI keys.

    :param values: INI keys such as ``dbname``/``user``/``password``/``host``/``port``.
    :return: a ``postgresql+psycopg://`` URL.
    """
    user = values.get("user", "")
    password = values.get("password", "")
    host = values.get("host", "localhost")
    port = values.get("port", "5432")
    dbname = values.get("dbname") or values.get("database", "")
    auth = user
    if password:
        auth = f"{user}:{password}"
    prefix = f"{auth}@" if auth else ""
    return f"postgresql+psycopg://{prefix}{host}:{port}/{dbname}"


class Settings(BaseSettings):
    """Resolved application settings.

    :ivar database_url: the SQLAlchemy database URL.
    :ivar log_level: the logging level name (e.g. ``"INFO"``).
    """

    model_config = SettingsConfigDict(env_prefix="PDBSEARCH_", env_file=".env", extra="ignore")

    database_url: str = ""
    log_level: str = "INFO"


def load_settings(
    database_url: str | None = None,
    log_level: str | None = None,
    config_path: Path | None = None,
    key_path: Path | None = None,
    section: str = "postgresql",
) -> Settings:
    """Load settings honouring the four-source priority order.

    CLI flags win, then ``PDBSEARCH_*`` env / ``.env`` (via pydantic-settings), then the
    Fernet-encrypted INI file as a fallback for any field still unset.

    :param database_url: explicit database URL from a CLI flag (highest priority).
    :param log_level: explicit log level from a CLI flag.
    :param config_path: path to the Fernet-encrypted INI file (lowest priority source).
    :param key_path: path to the Fernet key file.
    :param section: the INI section to read.
    :return: a fully resolved :class:`Settings`.
    :raises FileNotFoundError: if a key file is required but missing.
    :raises ValueError: if the INI section is absent.
    """
    overrides: dict[str, Any] = {}
    if database_url:
        overrides["database_url"] = database_url
    if log_level:
        overrides["log_level"] = log_level
    settings = Settings(**overrides)

    if not settings.database_url and config_path is not None and key_path is not None:
        ini = _decrypt_ini(config_path, key_path, section)
        settings = settings.model_copy(update={"database_url": _url_from_ini(ini)})

    return settings
