"""Alembic migration environment.

The database URL is taken from Alembic's ``x`` argument
(``-x database_url=...``) when present, otherwise from the application
:class:`~paper_sorts.config.Settings` chain. ``target_metadata`` is the ORM
``Base.metadata`` so autogenerate and online migrations share one schema source.
"""

from __future__ import annotations

from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from paper_sorts.config import load_settings
from paper_sorts.db.models import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _resolve_url() -> str:
    """Return the database URL from the config, ``-x`` args, or app settings.

    Priority: an explicit ``sqlalchemy.url`` main option (set by the ``migrate``
    subcommand) > an Alembic ``-x database_url=...`` argument > the application
    settings chain.
    """
    configured = config.get_main_option("sqlalchemy.url")
    if configured:
        return configured
    x_args = context.get_x_argument(as_dictionary=True)
    if "database_url" in x_args:
        return x_args["database_url"]
    return load_settings().database_url


def run_migrations_offline() -> None:
    """Run migrations without a live DBAPI connection (emit SQL)."""
    context.configure(
        url=_resolve_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations against a live database connection."""
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
