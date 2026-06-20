"""Alembic environment configuration for paper_sorts.

Reads the database URL from the ``PDBSEARCH_DATABASE_URL`` environment
variable (or from ``alembic.ini``'s ``sqlalchemy.url`` key).
"""

from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

from paper_sorts.db.models import Base  # noqa: E402

target_metadata = Base.metadata


def _get_url() -> str:
    """Return the database URL from environment or alembic.ini."""
    env_url = os.environ.get("PDBSEARCH_DATABASE_URL")
    if env_url:
        return env_url
    return config.get_main_option("sqlalchemy.url") or ""


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (emit SQL, no live DB)."""
    url = _get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode (live database connection required)."""
    cfg_section = config.get_section(config.config_ini_section) or {}
    url = _get_url()
    if url:
        cfg_section["sqlalchemy.url"] = url

    connectable = engine_from_config(
        cfg_section,
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
