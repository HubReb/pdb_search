"""Configuration for paper_sorts using pydantic-settings v2.

Loads settings from four sources in priority order (highest first):
1. CLI flags (--database-url, --log-level, --config, --key)
2. Environment variables with PDBSEARCH_ prefix
3. .env file in the working directory
4. Fernet-encrypted INI file (--config + --key)
"""

from __future__ import annotations

import logging
from configparser import ConfigParser
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet, InvalidToken
from pydantic import SecretStr
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict


class FernetIniSource(PydanticBaseSettingsSource):
    """Custom pydantic-settings source for a Fernet-encrypted INI config file.

    Reads the config_file and key_file fields from already-resolved settings,
    decrypts the file, and returns database connection parameters.

    :raises FileNotFoundError: if config_file exists but key_file is missing
    :raises ValueError: if the key file cannot decrypt the config file
    """

    def __init__(self, settings_cls: type[BaseSettings]) -> None:
        """Initialise the Fernet INI source.

        :param settings_cls: the Settings class being constructed
        :type settings_cls: type[BaseSettings]
        """
        super().__init__(settings_cls)

    def get_fields_value(self) -> dict[str, Any]:
        """Read and decrypt the INI file; return a dict of field values.

        :return: mapping of field names to their values from the encrypted INI
        :rtype: dict[str, Any]
        :raises FileNotFoundError: if key_file is not specified or does not exist
        :raises ValueError: if decryption fails (wrong key or corrupt file)
        """
        # Pull config_file / key_file from env / defaults already resolved at a lower priority.
        # We read them directly from the environment/init_kwargs passed at construction time.
        init_data = self.init_kwargs  # type: ignore[attr-defined]
        config_file: Path | None = init_data.get("config_file")
        key_file: Path | None = init_data.get("key_file")

        if config_file is None:
            return {}

        config_path = Path(config_file)
        if not config_path.exists():
            return {}

        if key_file is None or not Path(key_file).exists():
            raise FileNotFoundError(
                f"Cannot decrypt config: key file not found. Check --key path. "
                f"(looked for: {key_file})"
            )

        try:
            encrypted = config_path.read_bytes()
            key_bytes = Path(key_file).read_bytes()
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
            # Map INI keys to Settings field names
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

    def __call__(self) -> dict[str, Any]:
        """Return field values from the Fernet INI source.

        :return: mapping of field names to values from the encrypted INI file
        :rtype: dict[str, Any]
        """
        try:
            return self.get_fields_value()
        except (FileNotFoundError, ValueError):
            raise
        except Exception:  # noqa: BLE001
            return {}

    def field_is_required(self, field_name: str, *args: Any, **kwargs: Any) -> bool:  # type: ignore[override]
        """Return False — all fields from this source are optional.

        :param field_name: name of the field
        :type field_name: str
        :return: always False (no field is required from this source)
        :rtype: bool
        """
        return False


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
        :param file_secret_settings: settings from file secrets
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
