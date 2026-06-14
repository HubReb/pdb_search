"""Configuration for paper_sorts.

Priority order (highest first):
    1. CLI flags (passed as overrides after construction)
    2. Environment variables prefixed PDBSEARCH_
    3. .env file in the working directory
    4. Fernet-encrypted INI file (config + key paths)
"""

from __future__ import annotations

import logging
from configparser import ConfigParser
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet, InvalidToken
from pydantic import field_validator
from pydantic.fields import FieldInfo
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


class FernetIniSettingsSource(PydanticBaseSettingsSource):
    """Custom pydantic-settings source that reads a Fernet-encrypted INI file.

    The encrypted file must contain an INI section named ``[postgresql]`` with
    ``host``, ``port``, ``dbname``, ``user``, and ``password`` fields. The
    decrypted content is used to build a ``database_url`` DSN.

    :param settings_cls: the Settings class being instantiated.
    :param config_path: path to the encrypted config file.
    :param key_path: path to the Fernet key file.
    :param section: INI section to read (default: ``postgresql``).
    """

    def __init__(
        self,
        settings_cls: type[BaseSettings],
        config_path: str | Path | None,
        key_path: str | Path | None,
        section: str = "postgresql",
    ) -> None:
        super().__init__(settings_cls)
        self._config_path = Path(config_path) if config_path else None
        self._key_path = Path(key_path) if key_path else None
        self._section = section

    def _decrypt(self) -> dict[str, Any]:
        """Decrypt the INI file and return a dict of settings values.

        :returns: dict with a ``database_url`` key if the file is readable.
        :raises FileNotFoundError: if the config or key file is missing.
        :raises InvalidToken: if the key does not match the encrypted file.
        :raises ValueError: if the expected INI section is absent.
        """
        if not self._config_path or not self._key_path:
            return {}
        if not self._config_path.exists() or not self._key_path.exists():
            return {}

        encrypted = self._config_path.read_bytes()
        key = self._key_path.read_bytes()
        try:
            fernet = Fernet(key)
            plaintext = fernet.decrypt(encrypted).decode("utf-8")
        except (InvalidToken, ValueError) as exc:
            raise ValueError(
                f"Could not decrypt config file '{self._config_path}': {exc}. "
                "Verify that the key file matches the encrypted config."
            ) from exc

        parser = ConfigParser()
        parser.read_string(plaintext)
        if not parser.has_section(self._section):
            raise ValueError(
                f"Section '[{self._section}]' not found in decrypted config."
            )

        params = dict(parser.items(self._section))
        host = params.get("host", "localhost")
        port = params.get("port", "5432")
        dbname = params.get("dbname", params.get("database", ""))
        user = params.get("user", "")
        password = params.get("password", "")
        url = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{dbname}"
        return {"database_url": url}

    def get_field_value(
        self,
        field: FieldInfo,
        field_name: str,
    ) -> tuple[Any, str, bool]:
        """Return the value for *field_name* from the decrypted INI source.

        Required by the :class:`PydanticBaseSettingsSource` ABC.

        :param field: pydantic FieldInfo descriptor.
        :param field_name: name of the field being resolved.
        :returns: ``(value, field_key, value_is_complex)`` tuple as required by
            pydantic-settings; returns ``(None, field_name, False)`` when no
            value is available for this field from the encrypted source.
        """
        data = self._decrypt()
        if field_name in data:
            return data[field_name], field_name, False
        return None, field_name, False

    def __call__(self) -> dict[str, Any]:
        return self._decrypt()


class Settings(BaseSettings):
    """Application settings loaded from four sources in priority order.

    :param database_url: SQLAlchemy-compatible PostgreSQL DSN.
    :param log_level: logging level string (DEBUG, INFO, WARNING, ERROR).
    :param config: path to the Fernet-encrypted INI config file.
    :param key: path to the Fernet key file.
    :param section: INI section name to read (default: ``postgresql``).
    """

    model_config = SettingsConfigDict(
        env_prefix="PDBSEARCH_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = ""
    log_level: str = "INFO"
    config: str = "../../database.crypt"
    key: str = "../../key"
    section: str = "postgresql"

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate that ``log_level`` is a known stdlib logging level name.

        :param v: raw level string.
        :returns: upper-cased level string.
        :raises ValueError: if ``v`` is not a valid logging level.
        """
        upper = v.upper()
        if upper not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            raise ValueError(
                f"Invalid log level '{v}'. Must be one of: DEBUG, INFO, WARNING, ERROR, CRITICAL."
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

        Priority: init (CLI overrides) > env vars > .env file > encrypted INI.

        :param settings_cls: the Settings class.
        :param init_settings: programmatic overrides (CLI flags).
        :param env_settings: environment variable source.
        :param dotenv_settings: .env file source.
        :param file_secret_settings: unused default secrets source.
        :returns: tuple of sources in priority order.
        """
        # We create the FernetIniSettingsSource lazily here using the *current*
        # values of config/key as found in the lower-priority sources. Because
        # pydantic-settings evaluates sources left-to-right and merges, we must
        # extract config/key defaults ourselves for the Fernet source.
        config_path = "../../database.crypt"
        key_path = "../../key"
        section = "postgresql"

        fernet_source = FernetIniSettingsSource(
            settings_cls,
            config_path=config_path,
            key_path=key_path,
            section=section,
        )
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            fernet_source,
        )

    def get_log_level_int(self) -> int:
        """Return the integer value of the configured log level.

        :returns: integer logging level (e.g. ``logging.INFO``).
        """
        return getattr(logging, self.log_level, logging.INFO)
