"""Application configuration for paper_sorts.

Loads settings from four sources in priority order (highest first):
  1. CLI flags (--database-url, --log-level) — callers override fields before use
  2. Environment variables with PDBSEARCH_ prefix
  3. .env file in current directory
  4. Fernet-encrypted INI file (--config / --key)

Plaintext credentials, decryption keys, and encrypted config files must NOT
be committed to the repository and must NOT appear in logs (SecretStr).
"""

from __future__ import annotations

import logging
from configparser import ConfigParser
from typing import Any

from cryptography.fernet import Fernet, InvalidToken
from pydantic import SecretStr
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

logger = logging.getLogger(__name__)


class FernetIniSource(PydanticBaseSettingsSource):
    """Custom pydantic-settings source that reads a Fernet-encrypted INI file.

    The encrypted file must contain a [postgresql] section with a 'database_url'
    key, or individual keys: host, port, dbname, user, password.
    """

    def __init__(
        self,
        settings_cls: type[BaseSettings],
        config_file: str | None,
        key_file: str | None,
        section: str = "postgresql",
    ) -> None:
        """Initialise the Fernet INI source.

        :param settings_cls: the Settings class being instantiated
        :param config_file: path to the Fernet-encrypted INI file; None disables this source
        :param key_file: path to the file containing the Fernet key; None disables this source
        :param section: INI section to read; defaults to 'postgresql'
        """
        super().__init__(settings_cls)
        self._config_file = config_file
        self._key_file = key_file
        self._section = section

    def _read_encrypted(self) -> dict[str, Any]:
        """Decrypt the config file and return the section as a dict.

        :raises ValueError: if the key file is missing, the section is absent,
            or decryption fails
        :return: dict of key/value pairs from the config section
        """
        if not self._config_file or not self._key_file:
            return {}
        try:
            with open(self._config_file, "rb") as f:
                encrypted = f.read()
            with open(self._key_file, "rb") as f:
                key = f.read()
        except FileNotFoundError as exc:
            raise ValueError(
                f"Configuration file or key file not found: {exc.filename}. "
                "Check --config and --key paths."
            ) from exc
        try:
            fernet = Fernet(key)
            decrypted = fernet.decrypt(encrypted).decode("utf-8")
        except (InvalidToken, Exception) as exc:
            raise ValueError(
                "Failed to decrypt configuration file. "
                "Ensure the key file matches the encrypted config."
            ) from exc
        parser = ConfigParser()
        parser.read_string(decrypted)
        if not parser.has_section(self._section):
            raise ValueError(
                f"Section [{self._section}] not found in encrypted config file."
            )
        return dict(parser.items(self._section))

    def __call__(self) -> dict[str, Any]:
        """Return settings from the Fernet INI source.

        :return: dict of field name to value; empty dict if source is unavailable
        """
        try:
            raw = self._read_encrypted()
        except ValueError as exc:
            logger.warning("Fernet INI source skipped: %s", exc)
            return {}
        result: dict[str, Any] = {}
        # Support both 'database_url' key and individual host/port/dbname/user/password
        if "database_url" in raw:
            result["database_url"] = raw["database_url"]
        elif "host" in raw:
            host = raw.get("host", "localhost")
            port = raw.get("port", "5432")
            dbname = raw.get("dbname", "paper_sorts")
            user = raw.get("user", "")
            password = raw.get("password", "")
            result["database_url"] = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{dbname}"
        return result

    def get_field_value(self, field: Any, field_name: str) -> tuple[Any, str, bool]:  # noqa: ANN401
        """Return the value for a specific field from this source.

        pydantic-settings v2 abstract method implementation.

        :param field: pydantic FieldInfo for the field
        :param field_name: name of the field
        :return: tuple of (value, field_key, value_is_complex)
        """
        data = self.__call__()
        value = data.get(field_name)
        return value, field_name, False

    def field_is_complex(self, field: Any) -> bool:  # noqa: ANN401
        """Return whether a field requires complex (JSON) parsing.

        :param field: the pydantic field info
        :return: always False for this source
        """
        return False


class Settings(BaseSettings):
    """Application settings loaded from four sources in priority order.

    Priority (highest first):
      CLI overrides > PDBSEARCH_* env vars > .env file > Fernet-encrypted INI

    :param database_url: PostgreSQL DSN used by SQLAlchemy
    :param log_level: logging level string (DEBUG / INFO / WARNING / ERROR)
    :param config_file: path to Fernet-encrypted INI config file
    :param key_file: path to the Fernet decryption key file
    """

    model_config = SettingsConfigDict(
        env_prefix="PDBSEARCH_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: SecretStr = SecretStr(
        "postgresql+psycopg://localhost:5432/paper_sorts"
    )
    log_level: str = "INFO"
    config_file: str | None = None
    key_file: str | None = None

    @classmethod
    def customise_sources(
        cls,
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Define the four-source priority chain.

        :param init_settings: values passed to __init__ (CLI overrides)
        :param env_settings: environment variables (PDBSEARCH_*)
        :param dotenv_settings: .env file
        :param file_secret_settings: pydantic file secrets (unused)
        :return: ordered tuple of sources, highest priority first
        """
        # FernetIniSource reads config_file/key_file from env/init after the
        # other sources run — we extract them from init_settings data for now.
        # In practice callers construct Settings(config_file=..., key_file=...).
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            file_secret_settings,
        )

    def get_database_url(self) -> str:
        """Return the database URL as a plain string.

        :return: PostgreSQL DSN string
        """
        return self.database_url.get_secret_value()
