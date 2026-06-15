"""Application configuration via pydantic-settings.

Settings are loaded from four sources, highest priority first:

1. **CLI flags** — passed as explicit init kwargs by ``cli/app.py``
   (``--database-url``, ``--log-level``, ``--log-file``).
2. **Environment variables** — ``PDBSEARCH_*``.
3. **`.env` file** — in the working directory.
4. **Fernet-encrypted INI file** — the legacy encrypted-config workflow,
   preserved as one supported source (FR-007). Enabled by pointing
   ``--config <path> --key <path>`` at an encrypted INI whose ``[postgresql]``
   section holds ``host/port/dbname/user/password``.

A missing or wrong key produces a clear, actionable error — never a stack
trace reaching the user.
"""

from __future__ import annotations

from configparser import ConfigParser
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet, InvalidToken
from pydantic import Field
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


class ConfigError(RuntimeError):
    """Raised when configuration cannot be loaded (e.g. a lost decryption key)."""


def _decrypt_ini(config_path: Path, key_path: Path, section: str = "postgresql") -> dict[str, str]:
    """Decrypt a Fernet-encrypted INI file and return one section as a dict.

    :param config_path: path to the Fernet-encrypted INI file.
    :param key_path: path to the Fernet key file.
    :param section: INI section to extract (defaults to ``postgresql``).
    :returns: the section's key/value pairs.
    :raises ConfigError: if a file is missing, the key is wrong, or the
        section is absent — each with an actionable message.
    """
    try:
        encrypted = config_path.read_bytes()
        key = key_path.read_bytes()
    except FileNotFoundError as exc:
        raise ConfigError(
            f"Encrypted-config source: file not found ({exc.filename}). "
            "Check the --config and --key paths."
        ) from exc
    try:
        decrypted = Fernet(key).decrypt(encrypted).decode("utf-8")
    except (InvalidToken, ValueError) as exc:
        raise ConfigError(
            "Encrypted-config source: could not decrypt the config with the "
            "supplied key. The key may be wrong or the file corrupt."
        ) from exc
    parser = ConfigParser()
    parser.read_string(decrypted)
    if not parser.has_section(section):
        raise ConfigError(
            f"Encrypted-config source: section [{section}] not found in {config_path}."
        )
    return dict(parser.items(section))


def _ini_to_url(params: dict[str, str]) -> str:
    """Build a SQLAlchemy URL from decrypted INI ``[postgresql]`` parameters.

    :param params: keys ``host``, ``port``, ``dbname`` (or ``database``),
        ``user``, ``password``.
    :returns: a ``postgresql+psycopg://`` URL.
    """
    host = params.get("host", "localhost")
    port = params.get("port", "5432")
    dbname = params.get("dbname") or params.get("database", "")
    user = params.get("user", "")
    password = params.get("password", "")
    auth = f"{user}:{password}@" if user else ""
    return f"postgresql+psycopg://{auth}{host}:{port}/{dbname}"


class _EncryptedIniSource(PydanticBaseSettingsSource):
    """A pydantic-settings source backed by the Fernet-encrypted INI file.

    Lowest priority of the four sources. Active only when both an encrypted
    config path and a key path are supplied. The paths are read from the
    settings class attributes ``_encrypted_config_path`` /
    ``_encrypted_key_path`` set by :func:`load_settings`.
    """

    def get_field_value(self, field: Any, field_name: str) -> tuple[Any, str, bool]:  # noqa: D102
        # Field-level resolution is unused; __call__ builds the whole dict.
        return None, field_name, False

    def __call__(self) -> dict[str, Any]:
        """Return ``{database_url: ...}`` if the encrypted source is configured.

        :returns: a (possibly empty) settings dict.
        """
        cfg = getattr(self.settings_cls, "_encrypted_config_path", None)
        key = getattr(self.settings_cls, "_encrypted_key_path", None)
        if not cfg or not key:
            return {}
        params = _decrypt_ini(Path(cfg), Path(key))
        return {"database_url": _ini_to_url(params)}


class Settings(BaseSettings):
    """Resolved application settings.

    :ivar database_url: SQLAlchemy connection URL.
    :ivar log_level: stdlib logging level name (e.g. ``INFO``).
    :ivar log_file: optional path for a file log sink.
    """

    model_config = SettingsConfigDict(
        env_prefix="PDBSEARCH_",
        env_file=".env",
        extra="ignore",
    )

    # Set transiently by load_settings() so the encrypted source can find them.
    _encrypted_config_path: str | None = None
    _encrypted_key_path: str | None = None

    database_url: str = Field(default="")
    log_level: str = Field(default="INFO")
    log_file: str | None = Field(default=None)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Order the four sources: CLI init > env > .env > encrypted INI.

        :returns: the source callables in descending priority.
        """
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            _EncryptedIniSource(settings_cls),
        )


def load_settings(
    *,
    database_url: str | None = None,
    log_level: str | None = None,
    log_file: str | None = None,
    config_path: str | None = None,
    key_path: str | None = None,
) -> Settings:
    """Load settings honouring the four-source priority chain.

    CLI-supplied values (the keyword arguments) take precedence; the encrypted
    INI source is consulted only when both ``config_path`` and ``key_path`` are
    given.

    :param database_url: explicit database URL (highest priority if set).
    :param log_level: explicit logging level.
    :param log_file: explicit file-log path.
    :param config_path: path to the Fernet-encrypted INI file.
    :param key_path: path to the Fernet key file.
    :returns: a resolved :class:`Settings`.
    :raises ConfigError: if the encrypted source is configured but unreadable.
    """
    init: dict[str, Any] = {}
    if database_url is not None:
        init["database_url"] = database_url
    if log_level is not None:
        init["log_level"] = log_level
    if log_file is not None:
        init["log_file"] = log_file

    # Stash encrypted-source paths on the class for the custom source to read.
    Settings._encrypted_config_path = config_path
    Settings._encrypted_key_path = key_path
    try:
        return Settings(**init)
    finally:
        Settings._encrypted_config_path = None
        Settings._encrypted_key_path = None
