"""Configuration for paper_sorts.

Loads settings from four sources in priority order (highest first):
1. CLI flags (passed explicitly at startup by cli/app.py)
2. Environment variables (prefixed ``PDBSEARCH_``)
3. ``.env`` file in the working directory
4. Fernet-encrypted INI file (``--config`` + ``--key`` flags)

The custom :class:`FernetIniSettingsSource` implements source 4.
"""

import logging
from configparser import ConfigParser
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet
from pydantic import Field
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource

logger = logging.getLogger(__name__)


class FernetIniSettingsSource(PydanticBaseSettingsSource):
    """Pydantic-settings source that reads a Fernet-encrypted INI file.

    The encrypted file contains a standard ``[postgresql]`` section with
    ``dbname``, ``user``, ``password``, ``host``, ``port`` keys.  These are
    assembled into a ``database_url`` value.

    :param settings_cls: The :class:`BaseSettings` class being populated.
    :param config_file: Path to the encrypted ``.crypt`` file.
    :param key_file: Path to the Fernet key file.
    :param section: INI section name (default ``"postgresql"``).
    """

    def __init__(
        self,
        settings_cls: type[BaseSettings],
        config_file: Path,
        key_file: Path,
        section: str = "postgresql",
    ) -> None:
        """Initialise the Fernet INI settings source."""
        super().__init__(settings_cls)
        self._config_file = config_file
        self._key_file = key_file
        self._section = section

    def get_field_value(  # type: ignore[override]
        self, field_name: str, field: Any
    ) -> tuple[Any, str, bool]:
        """Required override — not used directly; see __call__."""
        return None, field_name, False

    def __call__(self) -> dict[str, Any]:
        """Decrypt the INI file and return a settings dict.

        :returns: Dict with ``database_url`` key if decryption succeeds,
            or empty dict on any error.
        :raises SystemExit: If the config file exists but the key is wrong.
        """
        if not self._config_file.exists():
            logger.debug("Fernet config file %s not found, skipping", self._config_file)
            return {}
        if not self._key_file.exists():
            logger.error(
                "Config file %s found but key file %s is missing — "
                "cannot decrypt credentials. Pass --database-url or set "
                "PDBSEARCH_DATABASE_URL to proceed without the key file.",
                self._config_file,
                self._key_file,
            )
            return {}

        try:
            with self._config_file.open("rb") as fh:
                encrypted = fh.read()
            with self._key_file.open("rb") as fh:
                key = fh.read()
            fernet = Fernet(key)
            decrypted = fernet.decrypt(encrypted).decode("utf-8")
        except Exception as exc:
            logger.error("Failed to decrypt config file: %s", exc)
            return {}

        parser = ConfigParser()
        parser.read_string(decrypted)

        if not parser.has_section(self._section):
            logger.error(
                "Section '%s' not found in decrypted config", self._section
            )
            return {}

        params = dict(parser.items(self._section))
        dbname = params.get("dbname", "")
        user = params.get("user", "")
        password = params.get("password", "")
        host = params.get("host", "localhost")
        port = params.get("port", "5432")

        url = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{dbname}"
        return {"database_url": url}


class Settings(BaseSettings):
    """Application settings for paper_sorts.

    Fields
    ------
    database_url : str
        SQLAlchemy-compatible PostgreSQL URL.
    log_level : str
        Logging level string (DEBUG, INFO, WARNING, ERROR).
    config_file : Path | None
        Path to Fernet-encrypted INI config file.
    key_file : Path | None
        Path to Fernet decryption key.
    """

    database_url: str = Field(default="", alias="database_url")
    log_level: str = Field(default="INFO")
    config_file: Path | None = Field(default=None)
    key_file: Path | None = Field(default=None)

    model_config = {
        "env_prefix": "PDBSEARCH_",
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "populate_by_name": True,
        "extra": "ignore",
    }

    @classmethod
    def customise_sources(
        cls,
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Define the four-source priority chain.

        Priority (highest first): init > env > dotenv > Fernet INI.

        The Fernet source requires config_file and key_file — if they are
        not set by the time this is called, it returns an empty dict.
        """
        return (init_settings, env_settings, dotenv_settings, file_secret_settings)
