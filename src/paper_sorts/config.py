"""Application configuration for paper_sorts.

Implements a four-source priority chain (highest to lowest):
  1. CLI flags (caller sets attributes directly)
  2. Environment variables with PDBSEARCH_ prefix
  3. .env file (if present)
  4. Fernet-encrypted INI file (legacy config; config_file + key_file required)

Plaintext credentials, decryption keys, and encrypted config files MUST NOT be
committed to the repository or written to logs (constitution Stack & Constraints).
"""

from __future__ import annotations

import logging
from configparser import ConfigParser
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet
from pydantic import SecretStr, field_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

logger = logging.getLogger(__name__)


class FernetIniSettingsSource(PydanticBaseSettingsSource):
    """Pydantic-settings source that reads a Fernet-encrypted INI file.

    Decrypts ``config_file`` using ``key_file``, parses the INI [postgresql]
    section, and maps known keys to Settings fields.

    This source is lowest-priority (below env vars and .env) per the
    four-source priority chain.
    """

    def __init__(self, settings_cls: type[BaseSettings]) -> None:
        """Initialise without a settings instance (values resolved at call time)."""
        super().__init__(settings_cls)

    def get_field_value(  # type: ignore[override]
        self,
        field_name: str,
        field_info: Any,  # noqa: ANN401
    ) -> tuple[Any, str, bool]:
        """Required by PydanticBaseSettingsSource (not used in this impl)."""
        return None, field_name, False

    def __call__(self) -> dict[str, Any]:
        """Read and decrypt the INI file, returning a settings dict.

        :return: Dict of setting names to their values from the INI file.
                 Returns empty dict if config_file or key_file are not set.
        :raises FileNotFoundError: If config_file or key_file paths do not exist.
        :raises ValueError: If decryption fails or INI section is missing.
        """
        # Peek at the current env/env-file values to find file paths
        import os

        config_file_raw = os.environ.get("PDBSEARCH_CONFIG_FILE")
        key_file_raw = os.environ.get("PDBSEARCH_KEY_FILE")

        if not config_file_raw or not key_file_raw:
            return {}

        config_path = Path(config_file_raw)
        key_path = Path(key_file_raw)

        if not config_path.exists():
            raise FileNotFoundError(
                f"Encrypted config file not found: {config_path}. "
                "Set PDBSEARCH_CONFIG_FILE to a valid path or remove the variable."
            )
        if not key_path.exists():
            raise FileNotFoundError(
                f"Decryption key file not found: {key_path}. "
                "Set PDBSEARCH_KEY_FILE to a valid path or remove the variable."
            )

        try:
            with open(config_path, "rb") as fh:
                encrypted = fh.read()
            with open(key_path, "rb") as fh:
                key = fh.read()
            fernet = Fernet(key)
            decrypted = fernet.decrypt(encrypted).decode("utf-8")
        except Exception as exc:
            raise ValueError(
                f"Failed to decrypt config file '{config_path}': {exc}"
            ) from exc

        parser = ConfigParser()
        parser.read_string(decrypted)
        section = "postgresql"
        if not parser.has_section(section):
            raise ValueError(
                f"Section '[{section}]' not found in decrypted config file '{config_path}'."
            )

        # Map INI keys to Settings field names
        key_map = {
            "host": "db_host",
            "port": "db_port",
            "dbname": "db_name",
            "database": "db_name",
            "user": "db_user",
            "password": "db_password",
        }
        result: dict[str, Any] = {}
        for ini_key, field_key in key_map.items():
            if parser.has_option(section, ini_key):
                result[field_key] = parser.get(section, ini_key)
        return result


class Settings(BaseSettings):
    """Application settings loaded from the four-source priority chain.

    Priority (highest first):
      1. Caller sets fields directly (e.g. CLI flags)
      2. PDBSEARCH_* environment variables
      3. .env file
      4. Fernet-encrypted INI file (config_file + key_file)

    :param db_host: PostgreSQL host.
    :param db_port: PostgreSQL port.
    :param db_name: PostgreSQL database name.
    :param db_user: PostgreSQL user.
    :param db_password: PostgreSQL password (stored as SecretStr).
    :param log_level: Logging level (e.g. INFO, DEBUG, WARNING).
    :param config_file: Path to Fernet-encrypted INI file (optional).
    :param key_file: Path to Fernet decryption key file (optional).
    """

    model_config = SettingsConfigDict(
        env_prefix="PDBSEARCH_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = ""
    db_user: str = ""
    db_password: SecretStr = SecretStr("")
    log_level: str = "INFO"
    config_file: Path | None = None
    key_file: Path | None = None

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate that log_level is a recognised Python logging level name.

        :param v: Raw log level string.
        :return: Upper-cased log level string.
        :raises ValueError: If v is not a valid logging level.
        """
        numeric = getattr(logging, v.upper(), None)
        if not isinstance(numeric, int):
            raise ValueError(
                f"Invalid log level '{v}'. Choose from: DEBUG, INFO, WARNING, ERROR, CRITICAL."
            )
        return v.upper()

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

        :param settings_cls: The Settings class.
        :param init_settings: Init-time (direct attribute) source.
        :param env_settings: Environment variable source.
        :param dotenv_settings: .env file source.
        :param file_secret_settings: Unused (file secrets not used here).
        :return: Ordered tuple of sources (highest priority first).
        """
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            FernetIniSettingsSource(settings_cls),
        )

    @property
    def database_url(self) -> str:
        """Build a SQLAlchemy-compatible psycopg v3 URL.

        :return: URL string of the form 'postgresql+psycopg://user:pass@host:port/dbname'.
        """
        password = self.db_password.get_secret_value()
        return (
            f"postgresql+psycopg://{self.db_user}:{password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )
