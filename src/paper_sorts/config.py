"""Configuration for paper_sorts via pydantic-settings.

Priority order (highest first):
1. CLI flags (callers override Settings fields directly).
2. Environment variables with prefix ``PDBSEARCH_``.
3. ``.env`` file in the working directory.
4. Fernet-encrypted INI file (``config_file`` + ``key_file``).

Usage::

    from paper_sorts.config import Settings

    settings = Settings()          # reads from env / .env
    settings = Settings(
        database_url="postgresql+psycopg://...",
        log_level="DEBUG",
    )

The Fernet source is attempted only when both ``config_file`` and
``key_file`` are set.  If ``key_file`` is missing a clear
:class:`FileNotFoundError` is raised with a helpful message.
"""

from __future__ import annotations

import logging
from configparser import ConfigParser
from typing import Any

from cryptography.fernet import Fernet, InvalidToken
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource

logger = logging.getLogger(__name__)


class FernetSettingsSource(PydanticBaseSettingsSource):
    """Custom pydantic-settings source that reads a Fernet-encrypted INI file.

    :param settings_cls: The :class:`BaseSettings` subclass being constructed.
    :param config_file: Path to the encrypted INI file.
    :param key_file: Path to the Fernet key file.
    :param section: INI section to read (default ``"postgresql"``).
    :raises FileNotFoundError: If *key_file* does not exist.
    :raises ValueError: If decryption fails (wrong key or corrupted file).
    """

    def __init__(
        self,
        settings_cls: type[BaseSettings],
        config_file: str,
        key_file: str,
        section: str = "postgresql",
    ) -> None:
        super().__init__(settings_cls)
        self._config_file = config_file
        self._key_file = key_file
        self._section = section

    def _load(self) -> dict[str, Any]:
        """Decrypt and parse the INI file; return a dict of key→value pairs."""
        try:
            with open(self._key_file, "rb") as f:
                key = f.read()
        except FileNotFoundError as exc:
            raise FileNotFoundError(
                f"Key file not found: {self._key_file!r}. "
                "Check the --key option or PDBSEARCH_KEY_FILE env var."
            ) from exc

        try:
            with open(self._config_file, "rb") as f:
                encrypted = f.read()
        except FileNotFoundError as exc:
            raise FileNotFoundError(
                f"Config file not found: {self._config_file!r}. "
                "Check the --config option."
            ) from exc

        try:
            fernet = Fernet(key)
            plaintext = fernet.decrypt(encrypted).decode("utf-8")
        except (InvalidToken, Exception) as exc:
            raise ValueError(
                f"Failed to decrypt config file {self._config_file!r}: {exc}"
            ) from exc

        parser = ConfigParser()
        parser.read_string(plaintext)

        if not parser.has_section(self._section):
            raise ValueError(
                f"Section [{self._section!r}] not found in decrypted config"
            )

        raw = dict(parser.items(self._section))
        # Map INI keys to Settings field names
        result: dict[str, Any] = {}
        if "host" in raw and "port" in raw and "database" in raw:
            result["database_url"] = (
                f"postgresql+psycopg://"
                f"{raw.get('user', '')}:{raw.get('password', '')}"
                f"@{raw['host']}:{raw['port']}/{raw['database']}"
            )
        elif "database_url" in raw:
            result["database_url"] = raw["database_url"]
        return result

    def get_field_value(
        self, field: Any, field_name: str
    ) -> tuple[Any, str, bool]:
        """Return (value, field_key, value_is_complex) for pydantic-settings."""
        data = self._load()
        val = data.get(field_name)
        return val, field_name, False

    def __call__(self) -> dict[str, Any]:
        """Return all settings from the Fernet source."""
        try:
            return self._load()
        except (FileNotFoundError, ValueError) as exc:
            logger.warning("Fernet config source unavailable: %s", exc)
            return {}


class Settings(BaseSettings):
    """Application-wide configuration.

    :param database_url: SQLAlchemy connection URL.
        Defaults to ``""`` — callers must supply a non-empty value or the
        application will fail when trying to connect.
    :param log_level: Logging level name (e.g. ``"INFO"``, ``"DEBUG"``).
    :param config_file: Optional path to a Fernet-encrypted INI config file.
    :param key_file: Optional path to the Fernet decryption key file.
    """

    model_config = {
        "env_prefix": "PDBSEARCH_",
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }

    database_url: str = Field(default="")
    log_level: str = Field(default="INFO")
    config_file: str | None = Field(default=None)
    key_file: str | None = Field(default=None)

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate that *v* is a recognised logging level name.

        :param v: Candidate log level string.
        :returns: Upper-cased level name.
        :raises ValueError: If *v* is not a valid logging level.
        """
        upper = v.upper()
        if upper not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            raise ValueError(f"Invalid log level: {v!r}")
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
        """Return source priority: init > env > .env > Fernet INI.

        The Fernet source is only added when both ``config_file`` and
        ``key_file`` are provided as init kwargs.
        """
        # We need to check init kwargs for config_file/key_file
        # Build without Fernet first to discover config_file/key_file values
        init_data: dict[str, Any] = init_settings()
        env_data: dict[str, Any] = env_settings()

        config_file = init_data.get("config_file") or env_data.get("config_file")
        key_file = init_data.get("key_file") or env_data.get("key_file")

        sources: tuple[PydanticBaseSettingsSource, ...] = (
            init_settings,
            env_settings,
            dotenv_settings,
        )
        if config_file and key_file:
            fernet_source = FernetSettingsSource(
                settings_cls,
                config_file=config_file,
                key_file=key_file,
            )
            sources = sources + (fernet_source,)

        return sources
