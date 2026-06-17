"""Application configuration via pydantic-settings with a four-source chain.

Sources, highest priority first:

1. CLI flags (passed in as ``init`` kwargs by ``cli/app.py``)
2. environment variables (``PDBSEARCH_*``)
3. ``.env`` file
4. Fernet-encrypted INI file (a custom settings source)

The encrypted-INI source preserves the legacy credential workflow. A missing key
file or a failed decryption raises :class:`ConfigError` with an actionable
message rather than leaking a traceback to the user.
"""

from __future__ import annotations

from configparser import ConfigParser
from pathlib import Path
from typing import Any

from pydantic import Field
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


class ConfigError(Exception):
    """Raised when configuration cannot be loaded (e.g. lost decryption key)."""


def _decrypt_ini(config_path: Path, key_path: Path, section: str = "postgresql") -> dict[str, str]:
    """Decrypt a Fernet-encrypted INI file and return one section as a dict.

    :param config_path: path to the encrypted INI file.
    :param key_path: path to the Fernet key file.
    :param section: INI section to read (defaults to ``postgresql``).
    :return: mapping of the section's keys to values.
    :raises ConfigError: if either file is missing, the key is wrong, or the
        section is absent.
    """
    from cryptography.fernet import Fernet, InvalidToken

    if not config_path.exists():
        raise ConfigError(f"Config file not found: {config_path}")
    if not key_path.exists():
        raise ConfigError(f"Key file not found: {key_path}")
    try:
        key = key_path.read_bytes()
        decrypted = Fernet(key).decrypt(config_path.read_bytes()).decode("utf-8")
    except (InvalidToken, ValueError) as exc:
        raise ConfigError("Could not decrypt config file — check that the key matches.") from exc
    parser = ConfigParser()
    parser.read_string(decrypted)
    if not parser.has_section(section):
        raise ConfigError(f"Section {section!r} not found in {config_path}")
    return dict(parser.items(section))


def _url_from_ini(values: dict[str, str]) -> str:
    """Assemble a SQLAlchemy URL from a decrypted INI section.

    :param values: INI section mapping (``host``/``port``/``dbname`` or
        ``database``/``user``/``password``).
    :return: a ``postgresql+psycopg://`` URL.
    """
    user = values.get("user", "")
    password = values.get("password", "")
    host = values.get("host", "localhost")
    port = values.get("port", "5432")
    dbname = values.get("dbname") or values.get("database", "")
    auth = f"{user}:{password}@" if user else ""
    return f"postgresql+psycopg://{auth}{host}:{port}/{dbname}"


class _EncryptedIniSource(PydanticBaseSettingsSource):
    """Lowest-priority settings source backed by a Fernet-encrypted INI file."""

    def get_field_value(self, field: Any, field_name: str) -> tuple[Any, str, bool]:
        """Unused per-field hook required by the base class.

        :param field: the model field.
        :param field_name: the field's name.
        :return: ``(None, field_name, False)`` — values come from ``__call__``.
        """
        return None, field_name, False

    def __call__(self) -> dict[str, Any]:
        """Return a ``database_url`` from the encrypted INI, if configured.

        :return: ``{"database_url": ...}`` when an INI is configured and decodes,
            otherwise an empty mapping.
        """
        data = self.current_state
        config_path = data.get("config_path")
        key_path = data.get("key_path")
        if not config_path or not key_path:
            return {}
        values = _decrypt_ini(Path(config_path), Path(key_path))
        return {"database_url": _url_from_ini(values)}


class Settings(BaseSettings):
    """Resolved application settings.

    :ivar database_url: SQLAlchemy URL for the PostgreSQL database.
    :ivar log_level: logging level name (e.g. ``"INFO"``).
    :ivar log_file: optional path for a file log sink.
    :ivar config_path: optional encrypted INI config path.
    :ivar key_path: optional Fernet key path for ``config_path``.
    """

    model_config = SettingsConfigDict(
        env_prefix="PDBSEARCH_",
        env_file=".env",
        extra="ignore",
    )

    database_url: str = Field(default="")
    log_level: str = Field(default="INFO")
    log_file: str | None = Field(default=None)
    config_path: str | None = Field(default=None)
    key_path: str | None = Field(default=None)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Order the config sources: init > env > .env > encrypted INI.

        :return: the source chain in descending priority order.
        """
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            _EncryptedIniSource(settings_cls),
        )

    def require_database_url(self) -> str:
        """Return the database URL or raise if it is unset.

        :return: the resolved database URL.
        :raises ConfigError: if no database URL could be resolved.
        """
        if not self.database_url:
            raise ConfigError(
                "No database URL configured. Pass --database-url, set "
                "PDBSEARCH_DATABASE_URL, or provide --config/--key."
            )
        return self.database_url
