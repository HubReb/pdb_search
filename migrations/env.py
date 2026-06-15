"""Alembic environment.

The database URL is taken from (in priority order) the ``-x url=...`` option passed to the
``alembic`` command, then the ``sqlalchemy.url`` config value, then the ``PDBSEARCH_DATABASE_URL``
environment variable resolved through :class:`paper_sorts.config.Settings`. This keeps the
migration tool aligned with the application's configuration layer.
"""

from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from paper_sorts.db.models import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _resolve_url() -> str:
    """Resolve the database URL for migrations.

    :return: a SQLAlchemy database URL.
    """
    x_args = context.get_x_argument(as_dictionary=True)
    if "url" in x_args:
        return x_args["url"]
    configured = config.get_main_option("sqlalchemy.url")
    if configured:
        return configured
    from paper_sorts.config import load_settings

    return load_settings().database_url


def run_migrations_offline() -> None:
    """Run migrations in offline mode (emit SQL without a DBAPI connection)."""
    context.configure(
        url=_resolve_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in online mode against a live connection."""
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


if os.environ.get("ALEMBIC_OFFLINE") == "1" or context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
