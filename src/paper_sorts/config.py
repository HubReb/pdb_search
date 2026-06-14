"""Application configuration loaded via pydantic-settings.

Priority order (highest to lowest):
  1. CLI flags (passed in via Settings constructor or environment)
  2. Environment variables (PDBSEARCH_*)
  3. .env file in current directory
  4. Fernet-encrypted INI file (--config + --key)

Usage::

    from paper_sorts.config import Settings
    settings = Settings()
    print(settings.database_url)
"""

from __future__ import annotations

import configparser
import io
import logging
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet
from pydantic import Field
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource

logger = logging.getLogger(__name__)


class FernetIniSettingsSource(PydanticBaseSettingsSource):
    """Load DB credentials from a Fernet-encrypted INI file.

    Expects a [postgresql] section with keys: dbname, user, password,
    host (optional, default localhost), port (optional, default 5432).
    Constructs a postgresql+psycopg:// DSN from those fields.

    :param settings_cls: The Settings class being configured.
    :param config_path: Path to the encrypted INI file.
    :param key_path: Path to the Fernet key file.
    """

    def __init__(
        self,
        settings_cls: type[BaseSettings],
        config_path: Path | None,
        key_path: Path | None,
    ) -> None:
        """Initialise the settings source."""
        super().__init__(settings_cls)
        self._config_path = config_path
        self._key_path = key_path

    def _read_ini(self) -> dict[str, Any]:
        """Decrypt and parse the INI file, returning a dict of values."""
        if not self._config_path or not self._key_path:
            return {}
        try:
            key = Path(self._key_path).read_bytes().strip()
            encrypted = Path(self._config_path).read_bytes()
            plaintext = Fernet(key).decrypt(encrypted).decode()
            parser = configparser.ConfigParser()
            parser.read_file(io.StringIO(plaintext))
            if not parser.has_section("postgresql"):
                logger.warning("Encrypted config has no [postgresql] section")
                return {}
            section = dict(parser["postgresql"])
            host = section.get("host", "localhost")
            port = section.get("port", "5432")
            dbname = section.get("dbname", "")
            user = section.get("user", "")
            password = section.get("password", "")
            dsn = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{dbname}"
            return {"database_url": dsn}
        except FileNotFoundError as exc:
            logger.error("Config or key file not found: %s", exc)
            return {}
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to decrypt config: %s", exc)
            return {}

    def __call__(self) -> dict[str, Any]:
        """Return settings values from the encrypted INI file."""
        return self._read_ini()

    def get_field_value(self, field: Any, field_name: str) -> tuple[Any, str, bool]:
        """Return (value, field_key, is_complex) for a single field.

        Required abstract method from PydanticBaseSettingsSource.

        :param field: The pydantic FieldInfo object.
        :param field_name: The field name to look up.
        :returns: Tuple of (value, field_name, is_complex).
        """
        data = self._read_ini()
        value = data.get(field_name)
        return value, field_name, False

    def field_is_complex(self, field: Any) -> bool:
        """Return False — all fields from this source are simple scalars."""
        return False


class Settings(BaseSettings):
    """Application settings with four-source priority chain.

    Sources in priority order:
    1. Constructor kwargs / environment variables (PDBSEARCH_*)
    2. .env file
    3. Fernet-encrypted INI file (fernet_config_path + fernet_key_path)

    :param database_url: PostgreSQL DSN (postgresql+psycopg://...).
    :param log_level: Python logging level name.
    :param fernet_config_path: Path to Fernet-encrypted INI (optional).
    :param fernet_key_path: Path to Fernet key file (optional).
    """

    database_url: str = Field(default="", description="PostgreSQL DSN")
    log_level: str = Field(default="INFO", description="Python logging level")
    fernet_config_path: Path | None = Field(default=None, description="Encrypted INI path")
    fernet_key_path: Path | None = Field(default=None, description="Fernet key file path")

    model_config = {
        "env_prefix": "PDBSEARCH_",
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Return sources in priority order: init > env > .env > Fernet INI."""
        # Extract config/key paths from init to pass to FernetIniSettingsSource
        init_data = init_settings()
        config_path = init_data.get("fernet_config_path")
        key_path = init_data.get("fernet_key_path")
        # Also check env
        if config_path is None:
            import os

            cp = os.environ.get("PDBSEARCH_FERNET_CONFIG_PATH")
            config_path = Path(cp) if cp else None
        if key_path is None:
            import os

            kp = os.environ.get("PDBSEARCH_FERNET_KEY_PATH")
            key_path = Path(kp) if kp else None

        fernet_source = FernetIniSettingsSource(settings_cls, config_path, key_path)
        return (init_settings, env_settings, dotenv_settings, fernet_source)
