"""Alembic migration environment.

Reads the database URL from PDBSEARCH_DATABASE_URL or constructs it from
PDBSEARCH_DB_* environment variables via the Settings model.
"""

from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

# Import ORM metadata so Alembic can see the models
from paper_sorts.db.models import Base  # noqa: E402

# Alembic Config object, provides access to alembic.ini values
config = context.config

# Set up logging from alembic.ini
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def get_url() -> str:
    """Get the database URL from environment or Settings.

    :return: PostgreSQL DSN
    :rtype: str
    :raises RuntimeError: if no database URL is configured
    """
    # Prefer explicit env var
    url = os.environ.get("PDBSEARCH_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if url:
        return url
    raise RuntimeError(
        "No database URL configured for Alembic. "
        "Set PDBSEARCH_DATABASE_URL or use `pdbsearch migrate --database-url`."
    )


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    Generates SQL without connecting to the database.
    """
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode (requires a live DB connection)."""
    configuration = config.get_section(config.config_ini_section) or {}
    configuration["sqlalchemy.url"] = get_url()
    connectable = engine_from_config(
        configuration,
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
