"""Application configuration via pydantic-settings.

Settings are resolved from four sources, highest priority first:

1. CLI flags (passed in as ``__init__`` keyword arguments),
2. environment variables prefixed ``PDBSEARCH_``,
3. a ``.env`` file in the working directory,
4. a Fernet-encrypted INI file (the legacy credential workflow), supplied via
   ``config`` + ``key`` paths.

A missing key or missing encrypted-config file raises
:class:`ConfigurationError` with an actionable message — never a raw traceback
(FR-007, Edge Cases).
"""

from __future__ import annotations

from configparser import ConfigParser
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet, InvalidToken
from pydantic import Field
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


class ConfigurationError(Exception):
    """Raised when configuration cannot be loaded (e.g. lost key, bad file)."""


def _build_url_from_section(items: dict[str, str]) -> str:
    """Assemble a SQLAlchemy URL from decrypted INI key/value pairs.

    :param items: the section's key/value pairs (host, port, dbname/database,
        user, password).
    :return: a ``postgresql+psycopg://…`` URL.
    """
    user = items.get("user", "")
    password = items.get("password", "")
    host = items.get("host", "localhost")
    port = items.get("port", "5432")
    dbname = items.get("dbname") or items.get("database") or ""
    auth = user
    if password:
        auth = f"{user}:{password}"
    prefix = f"{auth}@" if auth else ""
    return f"postgresql+psycopg://{prefix}{host}:{port}/{dbname}"


def load_encrypted_url(config_path: str, key_path: str, section: str = "postgresql") -> str:
    """Decrypt a Fernet INI config file and build a database URL from a section.

    :param config_path: path to the Fernet-encrypted INI file.
    :param key_path: path to the Fernet key file.
    :param section: the INI section to read.
    :return: a SQLAlchemy database URL.
    :raises ConfigurationError: if the file/key is missing, undecryptable, or the
        section is absent.
    """
    config_file = Path(config_path)
    key_file = Path(key_path)
    if not config_file.is_file():
        raise ConfigurationError(f"encrypted config file not found: {config_path}")
    if not key_file.is_file():
        raise ConfigurationError(f"decryption key file not found: {key_path}")
    try:
        fernet = Fernet(key_file.read_bytes())
        decrypted = fernet.decrypt(config_file.read_bytes()).decode("utf-8")
    except (InvalidToken, ValueError) as exc:
        raise ConfigurationError(
            "could not decrypt config — is the key file correct?"
        ) from exc
    parser = ConfigParser()
    parser.read_string(decrypted)
    if not parser.has_section(section):
        raise ConfigurationError(f"section {section!r} not found in config file")
    return _build_url_from_section(dict(parser.items(section)))


class EncryptedConfigSource(PydanticBaseSettingsSource):
    """A pydantic-settings source backed by a Fernet-encrypted INI file."""

    def get_field_value(self, field: Any, field_name: str) -> tuple[Any, str, bool]:
        """Unused per-field hook (the whole source is computed in ``__call__``)."""
        return None, field_name, False

    def __call__(self) -> dict[str, Any]:
        """Return ``{"database_url": ...}`` if a config+key pair is configured."""
        data = self.current_state
        config_path = data.get("config")
        key_path = data.get("key")
        if config_path and key_path and not data.get("database_url"):
            return {"database_url": load_encrypted_url(str(config_path), str(key_path))}
        return {}


class Settings(BaseSettings):
    """Resolved application settings.

    :ivar database_url: the SQLAlchemy database URL.
    :ivar log_level: the logging level name.
    :ivar log_file: optional path to a log file sink.
    :ivar config: optional path to a Fernet-encrypted INI config file.
    :ivar key: optional path to the Fernet key file.
    """

    model_config = SettingsConfigDict(
        env_prefix="PDBSEARCH_",
        env_file=".env",
        extra="ignore",
    )

    database_url: str = ""
    log_level: str = "INFO"
    log_file: str | None = None
    config: str | None = None
    key: str | None = None

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Order sources: init (CLI) > env > .env > encrypted INI.

        The encrypted-config source is consulted last so that an explicit
        ``database_url`` from any higher-priority source wins.
        """
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            EncryptedConfigSource(settings_cls),
        )


def load_settings(**overrides: Any) -> Settings:
    """Load settings, applying CLI overrides as the highest-priority source.

    :param overrides: CLI-provided values (e.g. ``database_url``, ``log_level``,
        ``config``, ``key``); ``None`` values are dropped so they do not shadow
        lower-priority sources.
    :return: a resolved :class:`Settings`.
    """
    cleaned = {k: v for k, v in overrides.items() if v is not None}
    return Settings(**cleaned)
