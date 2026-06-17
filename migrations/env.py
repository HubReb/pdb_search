"""Alembic environment.

The database URL is taken from the Alembic config's ``sqlalchemy.url`` when set
(e.g. by the ``migrate`` command), otherwise from the application settings. The
target metadata is the ORM models' metadata so ``--autogenerate`` and online
migrations see the canonical schema.
"""

from __future__ import annotations

from alembic import context
from sqlalchemy import engine_from_config, pool

from paper_sorts.config import Settings
from paper_sorts.db.models import Base

config = context.config

target_metadata = Base.metadata


def _database_url() -> str:
    """Resolve the database URL for migrations.

    :return: the configured URL (Alembic config wins, else app settings).
    """
    url = config.get_main_option("sqlalchemy.url")
    if url:
        return url
    return Settings().require_database_url()


def run_migrations_offline() -> None:
    """Run migrations in offline (URL-only) mode."""
    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations with a live engine connection."""
    section = config.get_section(config.config_ini_section, {})
    section["sqlalchemy.url"] = _database_url()
    connectable = engine_from_config(
        section, prefix="sqlalchemy.", poolclass=pool.NullPool
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
