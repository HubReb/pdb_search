"""Application configuration with a four-source priority chain.

Settings are resolved, highest priority first, from:

1. CLI flags (passed as init kwargs by :mod:`paper_sorts.cli.app`),
2. environment variables prefixed ``PDBSEARCH_``,
3. a ``.env`` file in the working directory,
4. a Fernet-encrypted INI file (custom source, lowest priority).

The encrypted-INI source preserves the legacy credential workflow: it decrypts
the INI with a key file and maps its ``[postgresql]`` section onto a
``database_url``. A missing key file produces a clear, actionable error rather
than a stack trace.
"""

from __future__ import annotations

from configparser import ConfigParser
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet, InvalidToken
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


class ConfigError(RuntimeError):
    """Raised when configuration cannot be loaded (e.g. a missing key file)."""


def _build_database_url(section: dict[str, str]) -> str:
    """Build a ``postgresql+psycopg://`` URL from an INI ``[postgresql]`` section.

    :param section: mapping of INI keys (``dbname``/``user``/``password``/
        ``host``/``port``) to values.
    :returns: a SQLAlchemy connection URL.
    """
    user = section.get("user", "")
    password = section.get("password", "")
    host = section.get("host", "localhost")
    port = section.get("port", "5432")
    dbname = section.get("dbname", section.get("database", ""))
    credentials = f"{user}:{password}@" if user else ""
    return f"postgresql+psycopg://{credentials}{host}:{port}/{dbname}"


def load_encrypted_database_url(config_path: Path, key_path: Path, section: str = "postgresql") -> str:
    """Decrypt a Fernet-encrypted INI and return its database URL.

    :param config_path: path to the encrypted INI file.
    :param key_path: path to the Fernet key file.
    :param section: INI section to read credentials from.
    :returns: a ``postgresql+psycopg://`` URL built from the section.
    :raises ConfigError: if either file is missing, the key is invalid, the
        ciphertext cannot be decrypted, or the section is absent.
    """
    if not config_path.exists():
        raise ConfigError(f"Encrypted config file not found: {config_path}")
    if not key_path.exists():
        raise ConfigError(
            f"Decryption key file not found: {key_path}. "
            "The encrypted config cannot be read without its key."
        )
    try:
        key = key_path.read_bytes()
        fernet = Fernet(key)
        decrypted = fernet.decrypt(config_path.read_bytes()).decode("utf-8")
    except (InvalidToken, ValueError) as exc:
        raise ConfigError(
            f"Could not decrypt {config_path} with {key_path}: the key may be wrong."
        ) from exc
    parser = ConfigParser()
    parser.read_string(decrypted)
    if not parser.has_section(section):
        raise ConfigError(f"Section [{section}] not found in {config_path}")
    return _build_database_url(dict(parser.items(section)))


class EncryptedIniSource(PydanticBaseSettingsSource):
    """Lowest-priority settings source backed by a Fernet-encrypted INI file.

    Active only when both ``config_file`` and ``key_file`` have been provided to
    the :class:`Settings` instance (typically via CLI flags).
    """

    def get_field_value(self, field: Any, field_name: str) -> tuple[Any, str, bool]:
        """Unused per-field hook; the whole mapping is produced in ``__call__``."""
        return None, field_name, False

    def __call__(self) -> dict[str, Any]:
        """Return ``{"database_url": …}`` decrypted from the INI, or ``{}``.

        :returns: the decrypted database URL mapping, or an empty mapping when
            no encrypted-config inputs were supplied.
        """
        data = self.current_state
        config_file = data.get("config_file")
        key_file = data.get("key_file")
        if not config_file or not key_file:
            return {}
        url = load_encrypted_database_url(Path(config_file), Path(key_file))
        return {"database_url": url}


class Settings(BaseSettings):
    """Resolved application settings.

    :ivar database_url: the SQLAlchemy connection URL.
    :ivar log_level: the logging level name (``DEBUG``/``INFO``/…).
    :ivar log_file: optional path for a file log sink.
    :ivar config_file: optional path to a Fernet-encrypted INI (CLI source).
    :ivar key_file: optional path to the Fernet key (CLI source).
    """

    model_config = SettingsConfigDict(
        env_prefix="PDBSEARCH_",
        env_file=".env",
        extra="ignore",
    )

    database_url: str = ""
    log_level: str = "INFO"
    log_file: str | None = None
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
        """Order the sources: init (CLI) > env > .env > encrypted INI.

        :returns: the source tuple in descending priority order.
        """
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            EncryptedIniSource(settings_cls),
        )

    def require_database_url(self) -> str:
        """Return the resolved database URL or fail with a clear message.

        :returns: a non-empty database URL.
        :raises ConfigError: if no source provided a database URL.
        """
        if not self.database_url:
            raise ConfigError(
                "No database URL configured. Provide --database-url, set "
                "PDBSEARCH_DATABASE_URL, add it to .env, or pass --config/--key."
            )
        return self.database_url


def load_settings(**cli_overrides: Any) -> Settings:
    """Load settings, applying CLI overrides as the highest-priority source.

    :param cli_overrides: keyword overrides from CLI flags (``None`` values are
        dropped so they do not shadow lower-priority sources).
    :returns: a resolved :class:`Settings` instance.
    """
    overrides = {k: v for k, v in cli_overrides.items() if v is not None}
    return Settings(**overrides)
