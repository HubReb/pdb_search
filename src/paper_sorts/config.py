"""Configuration for paper_sorts using pydantic-settings v2.

Loads settings from four sources in priority order (highest first):
1. CLI flags (--database-url, --log-level, --config, --key)
2. Environment variables with PDBSEARCH_ prefix
3. .env file in the working directory
4. Fernet-encrypted INI file (configured via config_file + key_file after other sources resolve)
"""

from __future__ import annotations

import logging
import os
from configparser import ConfigParser
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet, InvalidToken
from pydantic import SecretStr
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict


def _decrypt_fernet_ini(config_path: Path, key_path: Path) -> dict[str, Any]:
    """Decrypt a Fernet-encrypted INI file and return parsed PostgreSQL settings.

    :param config_path: path to the encrypted INI file
    :type config_path: Path
    :param key_path: path to the Fernet key file
    :type key_path: Path
    :return: dict of field names to values from the [postgresql] section
    :rtype: dict[str, Any]
    :raises FileNotFoundError: if key_path does not exist
    :raises ValueError: if decryption fails
    """
    if not key_path.exists():
        raise FileNotFoundError(
            f"Cannot decrypt config: key file not found. "
            f"Check --key path. (looked for: {key_path})"
        )
    try:
        encrypted = config_path.read_bytes()
        key_bytes = key_path.read_bytes()
        fernet = Fernet(key_bytes)
        decrypted = fernet.decrypt(encrypted).decode("utf-8")
    except InvalidToken as exc:
        raise ValueError(
            "Cannot decrypt config: the key file could not decrypt the config file. "
            "Verify --config and --key point to the correct files."
        ) from exc

    parser = ConfigParser()
    parser.read_string(decrypted)

    result: dict[str, Any] = {}
    section = "postgresql"
    if parser.has_section(section):
        mapping = dict(parser.items(section))
        if "dbname" in mapping:
            result["db_name"] = mapping["dbname"]
        if "user" in mapping:
            result["db_user"] = mapping["user"]
        if "password" in mapping:
            result["db_password"] = mapping["password"]
        if "host" in mapping:
            result["db_host"] = mapping["host"]
        if "port" in mapping:
            result["db_port"] = int(mapping["port"])
    return result


class FernetIniSource(PydanticBaseSettingsSource):
    """Custom pydantic-settings source for a Fernet-encrypted INI config file.

    Reads config_file and key_file from PDBSEARCH_CONFIG_FILE and PDBSEARCH_KEY_FILE
    environment variables (or from the Settings __init__ kwargs via env resolution).

    :raises FileNotFoundError: if config_file is specified but key_file is missing
    :raises ValueError: if the key file cannot decrypt the config file
    """

    def get_field_value(
        self,
        field: Any,
        field_name: str,
    ) -> tuple[Any, str, bool]:
        """Return the field value from the Fernet INI source.

        :param field: FieldInfo for the field being resolved
        :param field_name: name of the field
        :return: tuple of (value, field_key, value_is_complex)
        :rtype: tuple[Any, str, bool]
        """
        data = self._load_data()
        if field_name in data:
            return data[field_name], field_name, False
        return None, field_name, False

    def _load_data(self) -> dict[str, Any]:
        """Load and decrypt the INI file, returning a dict of field values.

        Reads PDBSEARCH_CONFIG_FILE and PDBSEARCH_KEY_FILE env vars directly
        (since they may not be resolved yet by the Settings source chain).

        :return: mapping of Settings field names to values
        :rtype: dict[str, Any]
        :raises FileNotFoundError: if key_file is not found
        :raises ValueError: if decryption fails
        """
        # Read config_file from env or Settings init kwargs
        # Try env var first, then init kwargs stored in the settings class
        config_file_str = os.environ.get("PDBSEARCH_CONFIG_FILE")
        key_file_str = os.environ.get("PDBSEARCH_KEY_FILE")

        # Also check Settings init_kwargs if available (passed via Settings(config_file=...))
        try:
            init_kw: dict[str, Any] = dict(self.init_kwargs)  # type: ignore[attr-defined]
            if "config_file" in init_kw and init_kw["config_file"] is not None:
                config_file_str = str(init_kw["config_file"])
            if "key_file" in init_kw and init_kw["key_file"] is not None:
                key_file_str = str(init_kw["key_file"])
        except AttributeError:
            pass

        if config_file_str is None:
            return {}

        config_path = Path(config_file_str)
        if not config_path.exists():
            return {}

        key_path = Path(key_file_str) if key_file_str else None
        if key_path is None:
            raise FileNotFoundError(
                "Cannot decrypt config: no key file specified. Use --key or "
                "set PDBSEARCH_KEY_FILE."
            )

        return _decrypt_fernet_ini(config_path, key_path)

    def __call__(self) -> dict[str, Any]:
        """Return all field values from the Fernet INI source.

        :return: mapping of field names to values from the encrypted INI file
        :rtype: dict[str, Any]
        """
        try:
            return self._load_data()
        except (FileNotFoundError, ValueError):
            raise
        except Exception:
            return {}


class Settings(BaseSettings):
    """Application settings loaded from four sources in priority order.

    Priority (highest first):
    1. Values passed explicitly (CLI flags, programmatic override)
    2. Environment variables (PDBSEARCH_*)
    3. .env file in the working directory
    4. Fernet-encrypted INI file (config_file + key_file)

    :param db_host: PostgreSQL host
    :param db_port: PostgreSQL port
    :param db_name: PostgreSQL database name
    :param db_user: PostgreSQL user
    :param db_password: PostgreSQL password (stored as SecretStr)
    :param config_file: path to Fernet-encrypted INI file (optional)
    :param key_file: path to Fernet key file (optional, required if config_file set)
    :param log_level: logging level string (default: INFO)
    :param log_file: optional file path for file logging
    :param database_url: full DSN overriding individual db_* fields
    """

    model_config = SettingsConfigDict(
        env_prefix="PDBSEARCH_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database connection
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = ""
    db_user: str = ""
    db_password: SecretStr = SecretStr("")

    # Optional encrypted config source
    config_file: Path | None = None
    key_file: Path | None = None

    # Logging
    log_level: str = "INFO"
    log_file: Path | None = None

    # Direct DSN override (highest priority when set)
    database_url: str | None = None

    @classmethod
    def customise_sources(
        cls,
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Configure source priority: init > env > dotenv > fernet-ini.

        :param init_settings: settings from __init__ kwargs
        :param env_settings: settings from environment variables
        :param dotenv_settings: settings from .env file
        :param file_secret_settings: settings from file secrets (unused)
        :return: tuple of sources in priority order
        :rtype: tuple[PydanticBaseSettingsSource, ...]
        """
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            FernetIniSource(cls),
        )

    def get_database_url(self) -> str:
        """Construct or return the PostgreSQL DSN.

        :return: full PostgreSQL connection DSN
        :rtype: str
        :raises ValueError: if neither database_url nor db_name/db_user are configured
        """
        if self.database_url:
            return self.database_url
        if not self.db_name or not self.db_user:
            raise ValueError(
                "Database not configured. Provide --database-url or set "
                "PDBSEARCH_DB_NAME and PDBSEARCH_DB_USER (or use --config/--key)."
            )
        password = self.db_password.get_secret_value()
        return (
            f"postgresql+psycopg://{self.db_user}:{password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )


_log_level_map: dict[str, int] = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


def parse_log_level(level: str) -> int:
    """Convert a log level string to a logging module integer constant.

    :param level: log level name (case-insensitive)
    :type level: str
    :return: logging module integer constant
    :rtype: int
    :raises ValueError: if the level string is not recognised
    """
    upper = level.upper()
    if upper not in _log_level_map:
        raise ValueError(
            f"Unknown log level '{level}'. "
            f"Valid values: {', '.join(_log_level_map.keys())}"
        )
    return _log_level_map[upper]
