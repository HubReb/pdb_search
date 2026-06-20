"""Application configuration via pydantic-settings.

Settings are loaded from four sources in priority order (highest first):

1. CLI flags (passed into :func:`load_settings`),
2. environment variables prefixed ``PDBSEARCH_``,
3. a ``.env`` file,
4. a Fernet-encrypted INI file (the legacy credential workflow), supplied via
   ``--config`` / ``--key``.

Secrets (plaintext credentials, decryption keys, encrypted files) are never
written to logs and never committed.
"""

from __future__ import annotations

from configparser import ConfigParser
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet, InvalidToken
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ConfigError(RuntimeError):
    """Raised when configuration cannot be assembled (clear, actionable)."""


def decrypt_ini(config_path: Path, key_path: Path, section: str = "postgresql") -> dict[str, str]:
    """Decrypt a Fernet-encrypted INI file and return one section as a dict.

    :param config_path: path to the Fernet-encrypted INI file.
    :param key_path: path to the Fernet key file.
    :param section: INI section to read (default ``"postgresql"``).
    :returns: the section's key/value pairs.
    :raises ConfigError: if a file is missing/unreadable, the key is invalid,
        or the section is absent — never a raw traceback.
    """
    try:
        encrypted = config_path.read_bytes()
        key = key_path.read_bytes()
    except OSError as exc:
        raise ConfigError(
            f"Could not read config or key file: {exc.filename or exc}. "
            "Check the --config and --key paths."
        ) from exc
    try:
        decrypted = Fernet(key).decrypt(encrypted).decode("utf-8")
    except (InvalidToken, ValueError) as exc:
        raise ConfigError(
            "Could not decrypt the config file — the key file does not match (lost or wrong key)."
        ) from exc
    parser = ConfigParser()
    parser.read_string(decrypted)
    if not parser.has_section(section):
        raise ConfigError(f"Section '{section}' not found in the config file.")
    return dict(parser.items(section))


def _url_from_ini(values: dict[str, str]) -> str:
    """Assemble a SQLAlchemy URL from decrypted INI key/value pairs."""
    user = values.get("user", "")
    password = values.get("password", "")
    host = values.get("host", "localhost")
    port = values.get("port", "5432")
    dbname = values.get("dbname") or values.get("database", "")
    auth = user + (f":{password}" if password else "")
    auth = f"{auth}@" if auth else ""
    return f"postgresql+psycopg://{auth}{host}:{port}/{dbname}"


class Settings(BaseSettings):
    """Resolved application settings."""

    model_config = SettingsConfigDict(
        env_prefix="PDBSEARCH_",
        env_file=".env",
        extra="ignore",
    )

    database_url: str | None = Field(default=None)
    log_level: str = Field(default="WARNING")
    log_file: str | None = Field(default=None)


def load_settings(
    *,
    database_url: str | None = None,
    log_level: str | None = None,
    log_file: str | None = None,
    config_path: Path | None = None,
    key_path: Path | None = None,
    section: str = "postgresql",
) -> Settings:
    """Resolve settings across the four-source priority chain.

    :param database_url: explicit CLI override (highest priority).
    :param log_level: explicit CLI override for the log level.
    :param log_file: explicit CLI override for an optional log file sink.
    :param config_path: Fernet-encrypted INI path (lowest-priority source).
    :param key_path: Fernet key path for ``config_path``.
    :param section: INI section to read.
    :returns: a fully resolved :class:`Settings`.
    :raises ConfigError: if the Fernet source is requested but unusable.
    """
    base = Settings()  # env + .env
    overrides: dict[str, Any] = {}

    if config_path is not None and key_path is not None and base.database_url is None:
        ini = decrypt_ini(config_path, key_path, section)
        overrides["database_url"] = _url_from_ini(ini)

    if database_url is not None:
        overrides["database_url"] = database_url
    if log_level is not None:
        overrides["log_level"] = log_level
    if log_file is not None:
        overrides["log_file"] = log_file

    return base.model_copy(update=overrides)
