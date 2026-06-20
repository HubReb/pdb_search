"""Configuration for paper-sorts.

Loads settings from four sources in priority order (highest first):
1. CLI flags / programmatic overrides
2. Environment variables (``PDBSEARCH_*``)
3. ``.env`` file in the current working directory
4. Fernet-encrypted INI file (when ``config_file`` and ``key_file`` are both set)

Usage::

    from paper_sorts.config import Settings

    settings = Settings()
    print(settings.database_url)
"""

from __future__ import annotations

import configparser
import logging
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet
from pydantic import PostgresDsn
from pydantic.fields import FieldInfo
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource

logger = logging.getLogger(__name__)


class FernetIniSettingsSource(PydanticBaseSettingsSource):
    """Custom pydantic-settings source that reads a Fernet-encrypted INI file.

    The source is skipped silently when ``config_file`` or ``key_file`` is
    *None* or when either file is missing, so plain-env and .env usage is
    unaffected.

    :param settings_cls: The ``Settings`` class this source belongs to.
    :param config_file: Path to the encrypted INI file (may be ``None``).
    :param key_file: Path to the Fernet key file (may be ``None``).
    """

    def __init__(
        self,
        settings_cls: type[BaseSettings],
        config_file: str | None,
        key_file: str | None,
    ) -> None:
        """Initialise the Fernet INI source.

        :param settings_cls: Settings class used by pydantic-settings.
        :param config_file: Filesystem path to the encrypted INI, or *None*.
        :param key_file: Filesystem path to the Fernet key, or *None*.
        """
        super().__init__(settings_cls)
        self._config_file = config_file
        self._key_file = key_file
        self._cached: dict[str, str] | None = None

    def _read_encrypted_config(self) -> dict[str, str]:
        """Decrypt the INI file and return the ``[postgresql]`` section as a dict.

        Results are cached after the first call.

        :returns: Mapping of configuration keys to values.
        """
        if self._cached is not None:
            return self._cached
        if not self._config_file or not self._key_file:
            self._cached = {}
            return self._cached
        cfg_path = Path(self._config_file)
        key_path = Path(self._key_file)
        if not cfg_path.exists() or not key_path.exists():
            logger.warning(
                "Fernet config or key file not found: %s / %s — skipping encrypted source",
                self._config_file,
                self._key_file,
            )
            self._cached = {}
            return self._cached
        try:
            encrypted = cfg_path.read_bytes()
            key = key_path.read_bytes()
            plaintext = Fernet(key).decrypt(encrypted).decode("utf-8")
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Failed to decrypt config file %s: %s — skipping encrypted source",
                self._config_file,
                exc,
            )
            self._cached = {}
            return self._cached
        parser = configparser.ConfigParser()
        parser.read_string(plaintext)
        section = "postgresql"
        if not parser.has_section(section):
            logger.error(
                "Section [%s] not found in encrypted config — skipping encrypted source",
                section,
            )
            self._cached = {}
            return self._cached
        items = dict(parser.items(section))
        # Build a PostgreSQL DSN from the legacy INI keys
        host = items.get("host", "localhost")
        port = items.get("port", "5432")
        dbname = items.get("dbname", items.get("database", ""))
        user = items.get("user", items.get("username", ""))
        password = items.get("password", "")
        dsn = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{dbname}"
        self._cached = {"database_url": dsn}
        return self._cached

    def get_field_value(
        self,
        field: FieldInfo,
        field_name: str,
    ) -> Any:
        """Return the value for *field_name* from the encrypted INI.

        :param field: pydantic ``FieldInfo`` for the field.
        :param field_name: Name of the settings field.
        :returns: Tuple of (value, field_key, value_is_complex) as expected by
            pydantic-settings.
        """
        data = self._read_encrypted_config()
        val = data.get(field_name)
        return val, field_name, False

    def __call__(self) -> dict[str, Any]:
        """Return all values from the Fernet-encrypted INI source.

        :returns: Dict of field name → value for fields present in the INI.
        """
        return self._read_encrypted_config()


class Settings(BaseSettings):
    """Application settings loaded from four sources.

    Priority order (highest first): CLI/programmatic overrides →
    environment variables (``PDBSEARCH_*``) → ``.env`` file →
    Fernet-encrypted INI (when ``config_file`` and ``key_file`` are set).

    :param database_url: PostgreSQL DSN used to connect to the database.
    :param log_level: Logging level string (``DEBUG``, ``INFO``, etc.).
    :param config_file: Path to the Fernet-encrypted INI config file.
    :param key_file: Path to the Fernet decryption key file.
    """

    model_config = {
        "env_prefix": "PDBSEARCH_",
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }

    database_url: PostgresDsn | None = None
    log_level: str = "INFO"
    config_file: str | None = None
    key_file: str | None = None

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Customise the settings source priority chain.

        Returns sources in priority order: init > env > dotenv > Fernet INI.

        :param settings_cls: The ``Settings`` class.
        :param init_settings: Source for values passed at construction time.
        :param env_settings: Source for ``PDBSEARCH_*`` environment variables.
        :param dotenv_settings: Source for ``.env`` file values.
        :param file_secret_settings: Source for file-based secrets (unused).
        :returns: Tuple of sources in priority order.
        """
        pre = cls._pre_read_config_paths()
        fernet_source = FernetIniSettingsSource(
            settings_cls,
            config_file=pre.get("config_file"),
            key_file=pre.get("key_file"),
        )
        return (init_settings, env_settings, dotenv_settings, fernet_source)

    @classmethod
    def _pre_read_config_paths(cls) -> dict[str, str | None]:
        """Read config_file and key_file from env/dotenv without full init.

        :returns: Dict with ``config_file`` and ``key_file`` keys.
        """
        import os

        from dotenv import dotenv_values

        dot = dotenv_values(".env")
        config_file = os.environ.get("PDBSEARCH_CONFIG_FILE") or dot.get(
            "PDBSEARCH_CONFIG_FILE"
        )
        key_file = os.environ.get("PDBSEARCH_KEY_FILE") or dot.get(
            "PDBSEARCH_KEY_FILE"
        )
        return {"config_file": config_file, "key_file": key_file}
