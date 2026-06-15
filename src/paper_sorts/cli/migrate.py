"""The ``migrate`` subcommand: upgrade a database to the canonical schema.

Runs Alembic to ``head`` against the configured database. On an empty database
this creates the schema from scratch; on a legacy ``bibtext_id`` database it
converges onto the canonical spelling. Idempotent — a re-run is a no-op.
"""

from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config

from paper_sorts.cli import prompts


def _alembic_config(database_url: str) -> Config:
    """Build an Alembic config pointed at this project's migrations.

    :param database_url: the SQLAlchemy URL to migrate.
    :return: a configured :class:`alembic.config.Config`.
    """
    project_root = Path(__file__).resolve().parents[3]
    config = Config(str(project_root / "alembic.ini"))
    config.set_main_option("script_location", str(project_root / "migrations"))
    config.set_main_option("sqlalchemy.url", database_url)
    config.attributes["database_url"] = database_url
    return config


def run_migrate(database_url: str) -> None:
    """Upgrade the configured database to the latest schema revision.

    :param database_url: the SQLAlchemy URL to migrate.
    """
    config = _alembic_config(database_url)
    command.upgrade(config, "head")
    prompts.info("Database migrated to the latest schema.")
