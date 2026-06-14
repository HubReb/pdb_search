"""Alembic environment configuration for paper_sorts migrations."""

import os
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool

from alembic import context

# Alembic Config object gives access to values within alembic.ini.
config = context.config

# Set up loggers from the ini config file.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Import ORM metadata for autogenerate support.
from paper_sorts.db.models import Base  # noqa: E402

target_metadata = Base.metadata

# Allow overriding the database URL via environment variable.
_db_url = os.environ.get("PDBSEARCH_DATABASE_URL")
if _db_url:
    config.set_main_option("sqlalchemy.url", _db_url)


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (no live DB connection required).

    Configures the context with a URL only and emits SQL to stdout.
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
    """Run migrations in 'online' mode (live DB connection).

    Creates an Engine and associates a connection with the migration context.
    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
