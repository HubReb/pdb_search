"""Alembic environment configuration for paper_sorts migrations."""

from __future__ import annotations

import os
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool

from alembic import context

# Alembic Config object provides access to values within alembic.ini.
config = context.config

# Set up loggers from alembic.ini if available.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Import ORM metadata so autogenerate can detect schema changes.
# This import must happen AFTER sys.path is set up via pythonpath in pytest config.
from paper_sorts.db.models import Base  # noqa: E402

target_metadata = Base.metadata

# Allow the DB URL to be overridden via an environment variable (used by tests).
_url_override = os.environ.get("PDBSEARCH_DATABASE_URL") or config.get_main_option(
    "sqlalchemy.url"
)
if _url_override:
    config.set_main_option("sqlalchemy.url", _url_override)


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (emit SQL without connecting).

    Configures the context with just a URL. Useful for generating migration SQL scripts.
    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode (connect and apply migrations directly).

    Creates an engine from the configuration and runs all pending migrations.
    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
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
