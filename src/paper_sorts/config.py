"""Configuration management for paper_sorts.

Loads settings from four sources in priority order (highest first):
1. CLI flags (injected by the Typer callback before Settings is instantiated)
2. Environment variables with prefix PDBSEARCH_
3. .env file in the current working directory
4. Fernet-encrypted INI file (when --config and --key are both provided)

Usage::

    from paper_sorts.config import Settings
    settings = Settings()
    print(settings.database_url)

For encrypted config::

    settings = Settings(config_file="database.crypt", key_file="key")
"""

from __future__ import annotations

import configparser
import logging
import os
from typing import Any

from cryptography.fernet import Fernet, InvalidToken
from pydantic import field_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

logger = logging.getLogger(__name__)


class FernetConfigSource(PydanticBaseSettingsSource):
    """Pydantic-settings source that reads a Fernet-encrypted INI configuration file.

    Decrypts the file using the key at ``key_file``, then parses the INI
    ``[postgresql]`` section and returns a ``database_url`` built from the
    ``host``, ``port``, ``dbname``, ``user``, ``password`` fields.

    Args:
        settings_cls: The Settings class being constructed.

    Raises:
        ValueError: If ``config_file`` or ``key_file`` does not exist, or if
            the key cannot decrypt the file (wrong key / corrupted file).
    """

    def get_field_value(self, field: Any, field_name: str) -> tuple[Any, str, bool]:
        """Return field value, field key, and whether it is complex.

        Required abstract method. Delegation to __call__ handles the actual
        field lookup.

        Args:
            field: Field info object.
            field_name: Name of the field being resolved.

        Returns:
            Tuple of (value, key, is_complex). Returns (None, field_name, False)
            when this source does not provide a value for the field.
        """
        data = self()
        value = data.get(field_name)
        return value, field_name, False

    def __call__(self) -> dict[str, Any]:
        """Load and decrypt the INI config file.

        Returns:
            Mapping of setting names to values extracted from the INI file,
            or an empty dict if config_file / key_file are not set.

        Raises:
            ValueError: On missing files, decryption failure, or missing section.
        """
        # Read config_file and key_file from environment (lowest-priority check)
        config_file = os.environ.get("PDBSEARCH_CONFIG") or ""
        key_file = os.environ.get("PDBSEARCH_KEY") or ""

        if not config_file or not key_file:
            return {}

        try:
            with open(config_file, "rb") as f:
                encrypted = f.read()
        except FileNotFoundError as exc:
            raise ValueError(
                f"Encrypted config file not found: {config_file!r}. "
                "Check the path passed to --config."
            ) from exc

        try:
            with open(key_file, "rb") as f:
                raw_key = f.read()
        except FileNotFoundError as exc:
            raise ValueError(
                f"Key file not found: {key_file!r}. "
                "Check the path passed to --key."
            ) from exc

        try:
            fernet = Fernet(raw_key)
            plaintext = fernet.decrypt(encrypted).decode("utf-8")
        except (InvalidToken, Exception) as exc:
            raise ValueError(
                "Failed to decrypt config file. The key file may be wrong or the "
                "config file may be corrupted. Check --config and --key paths."
            ) from exc

        parser = configparser.ConfigParser()
        parser.read_string(plaintext)
        section = "postgresql"
        if not parser.has_section(section):
            raise ValueError(
                f"Section [{section}] not found in decrypted config. "
                "The config file must contain a [postgresql] section."
            )

        params = dict(parser.items(section))
        # Build SQLAlchemy-compatible URL from INI fields
        user = params.get("user", "")
        password = params.get("password", "")
        host = params.get("host", "localhost")
        port = params.get("port", "5432")
        dbname = params.get("dbname", "") or params.get("database", "")
        url = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{dbname}"
        return {"database_url": url}

    def field_is_complex(self, field: Any) -> bool:
        """Return False — all fields from this source are simple scalars."""
        return False


class Settings(BaseSettings):
    """Application configuration loaded from the four-source priority chain.

    Priority order (highest to lowest):
    1. Values set directly on the instance (CLI callback injection)
    2. Environment variables prefixed PDBSEARCH_
    3. .env file
    4. Fernet-encrypted INI file (FernetConfigSource)

    Attributes:
        database_url: SQLAlchemy connection URL for PostgreSQL.
        log_level: Python logging level name (DEBUG, INFO, WARNING, ERROR).
        config_file: Path to Fernet-encrypted INI config (optional).
        key_file: Path to Fernet decryption key file (optional).
    """

    model_config = SettingsConfigDict(
        env_prefix="PDBSEARCH_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = ""
    log_level: str = "INFO"
    config_file: str = ""
    key_file: str = ""

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate that log_level is a recognised Python logging level name.

        Args:
            v: The log level string to validate.

        Returns:
            The uppercased log level name.

        Raises:
            ValueError: If the level name is not recognised by the logging module.
        """
        upper = v.upper()
        if upper not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            raise ValueError(
                f"Invalid log level {v!r}. Must be one of: DEBUG, INFO, WARNING, ERROR, CRITICAL"
            )
        return upper

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Define the four-source priority chain.

        Returns sources in priority order (highest first):
        init_settings > env_settings > dotenv_settings > FernetConfigSource.
        """
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            FernetConfigSource(settings_cls),
        )
