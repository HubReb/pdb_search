"""Configuration for paper_sorts using pydantic-settings v2.

Implements the four-source priority chain (highest to lowest):
  1. CLI args (injected by cli/app.py before Settings construction)
  2. Environment variables (PDBSEARCH_* prefix)
  3. .env file
  4. Fernet-encrypted INI file (--config + --key)

Only this module handles credentials.  Plaintext passwords and key material
must never be written to logs.
"""

from __future__ import annotations

import configparser
import logging
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet
from pydantic import field_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

logger = logging.getLogger(__name__)


class FernetIniSettingsSource(PydanticBaseSettingsSource):
    """Custom pydantic-settings source that reads a Fernet-encrypted INI file.

    The INI file must have a [postgresql] section with keys:
      dbname, user, password, host (optional), port (optional).

    Args:
        settings_cls: The Settings class being constructed.
        config_path: Path to the encrypted INI file.
        key_path: Path to the Fernet key file.
    """

    def __init__(
        self,
        settings_cls: type[BaseSettings],
        config_path: Path | None,
        key_path: Path | None,
    ) -> None:
        """Initialise and pre-load the decrypted INI data."""
        super().__init__(settings_cls)
        self._data: dict[str, Any] = {}
        if config_path is None or key_path is None:
            return
        try:
            key = key_path.read_bytes().strip()
            encrypted = config_path.read_bytes()
            plaintext = Fernet(key).decrypt(encrypted).decode()
            parser = configparser.ConfigParser()
            parser.read_string(plaintext)
            if parser.has_section("postgresql"):
                section = dict(parser["postgresql"])
                host = section.get("host", "localhost")
                port = section.get("port", "5432")
                dbname = section.get("dbname", "")
                user = section.get("user", "")
                password = section.get("password", "")
                self._data["database_url"] = (
                    f"postgresql+psycopg://{user}:{password}@{host}:{port}/{dbname}"
                )
        except FileNotFoundError as exc:
            raise ValueError(
                f"Config or key file not found: {exc.filename}. "
                "Check --config and --key paths."
            ) from exc
        except Exception as exc:
            raise ValueError(
                "Failed to decrypt config file. Verify the key file is correct."
            ) from exc

    def get_field_value(
        self,
        field: Any,
        field_name: str,
    ) -> tuple[Any, str, bool]:
        """Return the value for the given field from the decrypted INI.

        Args:
            field: The pydantic FieldInfo descriptor.
            field_name: The name of the field being resolved.

        Returns:
            Tuple of (value, field_key, value_is_complex).
        """
        value = self._data.get(field_name)
        return value, field_name, False

    def __call__(self) -> dict[str, Any]:
        """Return settings values (pydantic-settings v2 interface).

        Returns:
            Dict mapping field names to their values.
        """
        return self._data

    def field_is_complex(self, field: Any) -> bool:
        """Return False — all INI values are strings.

        Args:
            field: The field descriptor (unused).

        Returns:
            Always False.
        """
        return False


class Settings(BaseSettings):
    """Application settings loaded from four sources in priority order.

    Priority (highest first):
      1. Environment variables (PDBSEARCH_* prefix)
      2. .env file
      3. Fernet-encrypted INI file (set config_path + key_path before construction)

    CLI-level overrides are applied by cli/app.py by constructing Settings
    with explicit keyword args after parsing flags.

    Attributes:
        database_url: SQLAlchemy-compatible PostgreSQL connection string.
        log_level: Logging level string (DEBUG / INFO / WARNING / ERROR).
        config_path: Path to encrypted INI file (optional).
        key_path: Path to Fernet key file (optional).
    """

    model_config = SettingsConfigDict(
        env_prefix="PDBSEARCH_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = "postgresql+psycopg://localhost/paper_sorts"
    log_level: str = "INFO"
    config_path: Path | None = None
    key_path: Path | None = None

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate and normalise log_level to uppercase.

        Args:
            v: Raw log level string from the environment.

        Returns:
            Uppercased log level string.

        Raises:
            ValueError: If the string is not a valid logging level.
        """
        upper = v.upper()
        if upper not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError(f"Invalid log level: {v!r}")
        return upper

    @classmethod
    def customise_sources(
        cls,
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Define four-source priority chain.

        Args:
            init_settings: Settings provided at __init__ time (highest priority).
            env_settings: Settings from environment variables.
            dotenv_settings: Settings from .env file.
            file_secret_settings: Settings from secrets directory (not used).

        Returns:
            Tuple of sources in priority order (first wins).
        """
        return (init_settings, env_settings, dotenv_settings)
