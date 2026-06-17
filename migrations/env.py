"""Alembic migration environment.

The database URL is resolved from the ``sqlalchemy.url`` set in the config's
``attributes`` (when invoked programmatically by ``pdbsearch migrate``) or from
the ``PDBSEARCH_DATABASE_URL`` environment variable when run via the ``alembic``
CLI. ``target_metadata`` points at the ORM models so autogenerate works.
"""

from __future__ import annotations

import os

from alembic import context
from sqlalchemy import engine_from_config, pool

from paper_sorts.db.models import Base

config = context.config

target_metadata = Base.metadata


def _database_url() -> str:
    """Resolve the database URL for this migration run.

    :returns: the configured database URL.
    :raises RuntimeError: if no URL is available from any source.
    """
    url = config.attributes.get("sqlalchemy.url") or config.get_main_option("sqlalchemy.url")
    if not url:
        url = os.environ.get("PDBSEARCH_DATABASE_URL")
    if not url:
        raise RuntimeError("No database URL configured for Alembic.")
    return url


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode, emitting SQL to the script output."""
    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode against a live database connection."""
    section = config.get_section(config.config_ini_section, {})
    section["sqlalchemy.url"] = _database_url()
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
