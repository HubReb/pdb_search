"""Alembic environment script for paper-sorts migrations.

Reads the database URL from the ``Settings`` object (which in turn reads
from env vars / .env / Fernet INI).  The ``PDBSEARCH_DATABASE_URL``
environment variable is the primary override used by the CLI migrate command
and by tests.
"""

from __future__ import annotations

import os
import sys
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool

from alembic import context

# Make the src package importable when running alembic directly from the project root.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from paper_sorts.db.models import Base  # noqa: E402

# Alembic Config object — provides access to the .ini file in use.
config = context.config

# Set up loggers from the ini file if present.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def get_url() -> str:
    """Return the database URL from env override or alembic.ini.

    Reads ``PDBSEARCH_DATABASE_URL`` from the environment first, then falls
    back to the ``sqlalchemy.url`` in ``alembic.ini``.

    :returns: Database URL string.
    :raises RuntimeError: If no URL is configured anywhere.
    """
    url = os.environ.get("PDBSEARCH_DATABASE_URL") or config.get_main_option(
        "sqlalchemy.url"
    )
    if not url:
        raise RuntimeError(
            "Database URL not configured. Set PDBSEARCH_DATABASE_URL or "
            "sqlalchemy.url in alembic.ini."
        )
    return url


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (no DB connection required).

    Emits SQL to stdout; useful for generating migration scripts.
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
    config_section = config.get_section(config.config_ini_section, {})
    config_section["sqlalchemy.url"] = get_url()

    connectable = engine_from_config(
        config_section,
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
