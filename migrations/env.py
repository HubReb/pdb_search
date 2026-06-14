"""Alembic environment configuration for paper_sorts.

Reads the database URL from paper_sorts.config.Settings so that migration
runs use the same configuration chain as the application.
"""

from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

# Alembic Config object providing access to alembic.ini values.
config = context.config

# Interpret the config file for Python logging if present.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Import ORM metadata so Alembic can detect model changes.
from paper_sorts.db.models import Base  # noqa: E402

target_metadata = Base.metadata


def get_url() -> str:
    """Return the database URL from Settings or environment.

    Returns:
        A SQLAlchemy-compatible database URL string.
    """
    url = os.environ.get("PDBSEARCH_DATABASE_URL")
    if url:
        return url
    # Fall back to alembic.ini value
    return config.get_main_option("sqlalchemy.url", "postgresql+psycopg://localhost/paper_sorts")


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    Configures the context with just a URL rather than an engine.  This
    avoids even creating a DBAPI connection (useful for generating SQL scripts).
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
    """Run migrations in 'online' mode against a live database."""
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
