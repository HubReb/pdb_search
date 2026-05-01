"""Application settings — multi-source config with Fernet-INI fallback.

Source precedence (highest → lowest):

1. ``Settings(...)`` keyword arguments (i.e. CLI-driven init).
2. ``PDBSEARCH_*`` environment variables.
3. ``.env`` file in the working directory.
4. A Fernet-encrypted INI containing a ``[postgresql]`` section, contributed
   by :class:`FernetIniSource`.

Validation rules — both surface as plain-language ``ValueError`` instances,
never as stack traces from a downstream library:

* ``database_url`` must resolve from at least one source before the app starts.
* If ``fernet_config`` is set without ``fernet_key``, the load fails with
  ``"Fernet config requires a key file"`` (spec edge case "lost key").

The Fernet INI shape mirrors the legacy ``paper_sorts/config_reader.py``
exactly so existing personal databases keep working without re-encrypting:
section ``[postgresql]`` with keys ``dbname``, ``user``, ``password``,
``host`` (default ``localhost``), and ``port`` (default ``5432``). The values
are assembled into a SQLAlchemy URL with URL-quoted credentials so that
passwords containing ``@``, ``:``, ``/`` or ``%`` survive the round-trip.
"""

from __future__ import annotations

from configparser import ConfigParser
from pathlib import Path
from typing import Any
from urllib.parse import quote

from cryptography.fernet import Fernet, InvalidToken
from pydantic import model_validator
from pydantic.fields import FieldInfo
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


def _build_postgres_url(section: dict[str, str]) -> str:
    """Assemble a SQLAlchemy URL from a decoded ``[postgresql]`` INI section.

    Args:
        section: Mapping of INI keys (``dbname``/``user``/``password``/
            ``host``/``port``) to their string values.

    Returns:
        A ``postgresql+psycopg://...`` URL with URL-quoted credentials.
    """
    user = quote(section.get("user", ""), safe="")
    password = quote(section.get("password", ""), safe="")
    host = section.get("host", "localhost")
    port = section.get("port", "5432")
    dbname = section.get("dbname", "")
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{dbname}"


class FernetIniSource(PydanticBaseSettingsSource):
    """Decrypt a Fernet-encrypted INI and surface ``database_url`` from it.

    The source contributes a single field — ``database_url`` — built from the
    ``[postgresql]`` section. All other fields fall through to the higher
    priority sources.
    """

    def __init__(
        self,
        settings_cls: type[BaseSettings],
        fernet_config: Path | None,
        fernet_key: Path | None,
    ) -> None:
        """Capture the resolved fernet paths; defer reading until ``__call__``."""
        super().__init__(settings_cls)
        self._fernet_config = fernet_config
        self._fernet_key = fernet_key
        self._cached_url: str | None = None
        self._loaded = False

    def _decrypt(self) -> str | None:
        """Return the assembled URL or ``None`` if no Fernet config was supplied.

        Raises:
            ValueError: If ``fernet_config`` is set without ``fernet_key``,
                if the key cannot decrypt the config, or if the decoded INI
                has no ``[postgresql]`` section.
        """
        if self._loaded:
            return self._cached_url
        self._loaded = True
        if self._fernet_config is None:
            return None
        if self._fernet_key is None:
            msg = "Fernet config requires a key file"
            raise ValueError(msg)
        key = self._fernet_key.read_bytes()
        token = self._fernet_config.read_bytes()
        try:
            plaintext = Fernet(key).decrypt(token).decode("utf-8")
        except InvalidToken as exc:
            msg = "Fernet config could not be decrypted with the given key file"
            raise ValueError(msg) from exc
        cp = ConfigParser()
        cp.read_string(plaintext)
        if not cp.has_section("postgresql"):
            msg = "Fernet config must contain a [postgresql] section"
            raise ValueError(msg)
        self._cached_url = _build_postgres_url(dict(cp.items("postgresql")))
        return self._cached_url

    def get_field_value(self, field: FieldInfo, field_name: str) -> tuple[Any, str, bool]:
        """Return the decoded URL for ``database_url`` only; ``None`` otherwise."""
        if field_name != "database_url":
            return None, field_name, False
        return self._decrypt(), field_name, False

    def __call__(self) -> dict[str, Any]:
        """Yield ``{"database_url": ...}`` if a Fernet config was supplied."""
        url = self._decrypt()
        return {"database_url": url} if url is not None else {}


class Settings(BaseSettings):
    """Runtime configuration assembled from CLI / env / ``.env`` / Fernet INI."""

    model_config = SettingsConfigDict(
        env_prefix="PDBSEARCH_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str | None = None
    log_level: str = "INFO"
    log_file: Path | None = None
    fernet_config: Path | None = None
    fernet_key: Path | None = None

    @model_validator(mode="after")
    def _validate_resolved(self) -> Settings:
        """Enforce the lost-key rule and the database_url-required rule.

        Raises:
            ValueError: If ``fernet_config`` is set without ``fernet_key``,
                or if no source produced a ``database_url``.
        """
        if self.fernet_config is not None and self.fernet_key is None:
            msg = "Fernet config requires a key file"
            raise ValueError(msg)
        if not self.database_url:
            msg = (
                "database_url must be set via PDBSEARCH_DATABASE_URL, .env, "
                "Settings(database_url=...), or a Fernet-encrypted config "
                "(fernet_config + fernet_key)"
            )
            raise ValueError(msg)
        return self

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Add :class:`FernetIniSource` after env/dotenv but before file secrets.

        The Fernet paths themselves come from one of the higher-priority
        sources; we resolve them here so :class:`FernetIniSource` knows where
        to look without recursively reading from the very settings object
        that is still being constructed.
        """
        fernet_config: Path | None = None
        fernet_key: Path | None = None
        for source in (init_settings, env_settings, dotenv_settings):
            data = source()
            if fernet_config is None and data.get("fernet_config") is not None:
                fernet_config = Path(data["fernet_config"])
            if fernet_key is None and data.get("fernet_key") is not None:
                fernet_key = Path(data["fernet_key"])

        fernet_source = FernetIniSource(settings_cls, fernet_config, fernet_key)

        return (
            init_settings,
            env_settings,
            dotenv_settings,
            fernet_source,
            file_secret_settings,
        )
