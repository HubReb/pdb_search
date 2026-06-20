"""Alembic environment configuration for paper_sorts.

Reads the database URL from the PDBSEARCH_DATABASE_URL environment variable
(or a .env file) via paper_sorts.config.Settings, so no URL needs to be
hard-coded in alembic.ini.
"""

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

# Alembic Config object
config = context.config

# Interpret the config file for Python logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Import metadata so Alembic can detect schema changes
from paper_sorts.db.models import Base  # noqa: E402

target_metadata = Base.metadata


def get_url() -> str:
    """Return the database URL from environment or settings.

    :returns: SQLAlchemy-compatible PostgreSQL URL.
    :raises RuntimeError: If no database URL is configured.
    """
    url = os.environ.get("PDBSEARCH_DATABASE_URL", "")
    if url:
        return url
    # Fallback: try alembic.ini's sqlalchemy.url if set
    ini_url = config.get_main_option("sqlalchemy.url", default=None)
    if ini_url:
        return ini_url
    raise RuntimeError(
        "No database URL configured. "
        "Set PDBSEARCH_DATABASE_URL or pass --database-url."
    )


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (emit SQL to stdout).

    :raises RuntimeError: If database URL cannot be determined.
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
    """Run migrations against a live database connection.

    :raises RuntimeError: If database URL cannot be determined.
    """
    url = get_url()
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = url

    connectable = engine_from_config(
        configuration,
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
