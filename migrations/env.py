"""Alembic migration environment.

The target metadata is ``paper_sorts.db.models.Base.metadata``. The database
URL is resolved at runtime: an ``alembic -x database_url=...`` override, else
the ``sqlalchemy.url`` from ``alembic.ini``. Online mode only (PostgreSQL).
"""

from __future__ import annotations

from alembic import context
from sqlalchemy import engine_from_config, pool

from paper_sorts.db.models import Base

config = context.config

target_metadata = Base.metadata


def _resolve_url() -> str:
    """Return the database URL, honouring an ``-x database_url=...`` override.

    :returns: the SQLAlchemy URL to migrate.
    """
    x_args = context.get_x_argument(as_dictionary=True)
    override = x_args.get("database_url")
    if override:
        return override
    return config.get_main_option("sqlalchemy.url", "")


def run_migrations_online() -> None:
    """Run migrations against a live connection in transactional DDL."""
    url = _resolve_url()
    config.set_main_option("sqlalchemy.url", url)
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            transaction_per_migration=True,
        )
        with context.begin_transaction():
            context.run_migrations()


run_migrations_online()
