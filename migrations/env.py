"""Alembic environment for the paper-sorts modernization.

Resolves the database URL from :class:`paper_sorts.config.Settings` so that
the runtime source-precedence rules (CLI / env / .env / Fernet INI) apply
to ``alembic upgrade`` exactly as they do to the runtime CLI.

Test harnesses can pre-attach a live SQLAlchemy connection by setting
``config.attributes["connection"]`` before invoking ``command.upgrade(...)``;
when present, that connection is used directly so the migration runs inside
the harness's own transaction (the pattern pytest-postgresql uses for
fixture isolation).
"""

from logging.config import fileConfig
from typing import cast

from alembic import context
from sqlalchemy import engine_from_config, pool

from paper_sorts.config import Settings
from paper_sorts.db.models import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _resolve_url() -> str:
    """Return the URL from alembic.ini if set, else from Settings()."""
    ini_url = config.get_main_option("sqlalchemy.url")
    if ini_url:
        return ini_url
    # Settings()'s post-validator guarantees database_url is non-None; cast for mypy.
    return cast(str, Settings().database_url)


def run_migrations_offline() -> None:
    url = _resolve_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    pre_attached = config.attributes.get("connection")
    if pre_attached is not None:
        context.configure(connection=pre_attached, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()
        return

    section = config.get_section(config.config_ini_section, {})
    section["sqlalchemy.url"] = _resolve_url()
    connectable = engine_from_config(
        section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
