"""Configuration management for paper_sorts using pydantic-settings.

Priority order (highest to lowest):
  1. CLI flags (--database-url, --log-level)
  2. Environment variables (PDBSEARCH_*)
  3. .env file
  4. Fernet-encrypted INI file (--config + --key)
"""

from __future__ import annotations

import configparser
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet, InvalidToken
from pydantic import field_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


class FernetConfigSource(PydanticBaseSettingsSource):
    """Custom pydantic-settings source that reads a Fernet-encrypted INI file.

    The INI file must have a ``[postgresql]`` section with keys ``host``, ``port``,
    ``dbname``, ``user``, and ``password``. Those are assembled into a
    ``postgresql+psycopg://`` URL and returned as ``database_url``.

    :param settings_cls: The Settings class being constructed.
    :param config_path: Path to the Fernet-encrypted INI file.
    :param key_path: Path to the Fernet key file.
    :param section: INI section name to read (default: ``postgresql``).
    """

    def __init__(
        self,
        settings_cls: type[BaseSettings],
        config_path: Path | None,
        key_path: Path | None,
        section: str = "postgresql",
    ) -> None:
        """Initialise the source; store paths for lazy loading."""
        super().__init__(settings_cls)
        self._config_path = config_path
        self._key_path = key_path
        self._section = section

    def _decrypt_ini(self) -> dict[str, str]:
        """Decrypt the INI file and return a dict of its section's keys.

        :return: Mapping of INI keys to string values.
        :raises FileNotFoundError: If config_path or key_path do not exist.
        :raises ValueError: If decryption fails or section is missing.
        """
        if self._config_path is None or self._key_path is None:
            return {}
        if not self._config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self._config_path}")
        if not self._key_path.exists():
            raise FileNotFoundError(f"Key file not found: {self._key_path}")

        raw = self._config_path.read_bytes()
        key = self._key_path.read_bytes()
        try:
            fernet = Fernet(key)
            decrypted = fernet.decrypt(raw).decode("utf-8")
        except (InvalidToken, Exception) as exc:
            raise ValueError(
                f"Failed to decrypt config file {self._config_path}: {exc}"
            ) from exc

        parser = configparser.ConfigParser()
        parser.read_string(decrypted)
        if not parser.has_section(self._section):
            raise ValueError(
                f"Section [{self._section}] not found in decrypted config"
            )
        return dict(parser.items(self._section))

    def __call__(self) -> dict[str, Any]:
        """Return settings values sourced from the encrypted INI file.

        :return: Dict mapping ``database_url`` to the assembled PostgreSQL DSN.
        """
        try:
            ini = self._decrypt_ini()
        except (FileNotFoundError, ValueError):
            return {}

        if not ini:
            return {}

        host = ini.get("host", "localhost")
        port = ini.get("port", "5432")
        dbname = ini.get("dbname", "")
        user = ini.get("user", "")
        password = ini.get("password", "")

        if dbname and user:
            url = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{dbname}"
            return {"database_url": url}
        return {}

    def get_field_value(
        self, field: Any, field_name: str, field_info: Any = None
    ) -> tuple[Any, str, bool]:  # pragma: no cover
        """Not used directly; __call__ is used instead.

        :param field: Field to get value for.
        :param field_name: Name of the field.
        :param field_info: Optional field info.
        :return: Tuple of (value, field_key, value_is_complex).
        """
        return None, field_name, False


class Settings(BaseSettings):
    """Application settings loaded from the four-source priority chain.

    :param database_url: PostgreSQL DSN (e.g. ``postgresql+psycopg://user:pass@host/db``).
    :param log_level: Logging level string (default ``INFO``).
    :param config_path: Path to Fernet-encrypted INI config (optional).
    :param key_path: Path to Fernet key file (optional).
    :param ini_section: Section of the INI file to read (default ``postgresql``).
    """

    model_config = SettingsConfigDict(
        env_prefix="PDBSEARCH_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = ""
    log_level: str = "INFO"
    config_path: Path | None = None
    key_path: Path | None = None
    ini_section: str = "postgresql"

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate and normalise log_level to uppercase.

        :param v: Raw log level string.
        :return: Uppercased level string.
        :raises ValueError: If the value is not a valid Python logging level.
        """
        valid = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        upper = v.upper()
        if upper not in valid:
            raise ValueError(f"Invalid log level: {v!r}. Must be one of {valid}.")
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
        """Customise the source priority chain.

        Priority: init (CLI) > env > .env > file secret > defaults.

        :param settings_cls: The Settings class.
        :param init_settings: Source for ``__init__`` keyword arguments.
        :param env_settings: Source for environment variables.
        :param dotenv_settings: Source for ``.env`` file.
        :param file_secret_settings: Source for file-based secrets.
        :return: Tuple of sources in priority order (highest first).
        """
        # Priority: init kwargs (CLI) > env vars > .env file > secrets > defaults.
        # Fernet INI is resolved lazily via resolve_database_url() rather than
        # as a settings source, to keep error handling in the CLI layer.
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            file_secret_settings,
        )

    def get_fernet_source(self) -> FernetConfigSource:
        """Build a FernetConfigSource from the current config_path/key_path.

        :return: A FernetConfigSource instance ready to call.
        """
        return FernetConfigSource(
            type(self),
            self.config_path,
            self.key_path,
            self.ini_section,
        )

    def resolve_database_url(self) -> str:
        """Return the effective database URL, falling back to Fernet INI.

        :return: The database URL string.
        :raises ValueError: If no database URL can be resolved.
        """
        if self.database_url:
            return self.database_url
        fernet_values = self.get_fernet_source()()
        raw_url = fernet_values.get("database_url", "")
        url: str = str(raw_url) if raw_url else ""
        if not url:
            raise ValueError(
                "No database URL configured. Set PDBSEARCH_DATABASE_URL or "
                "provide --config and --key for the encrypted config."
            )
        return url
