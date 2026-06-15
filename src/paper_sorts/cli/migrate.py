"""Migrate subcommand.

Runs ``alembic upgrade head`` against the configured database. This creates the canonical
schema on a fresh database and converges either historical column-naming variant (``bibtex_id``
or the legacy ``bibtext_id`` typo) onto canonical. The operation is idempotent — a rerun is a
no-op once the database is at ``head``.
"""

from __future__ import annotations

import logging
from pathlib import Path

from alembic import command
from alembic.config import Config

from paper_sorts.cli import prompts

logger = logging.getLogger(__name__)


def _alembic_config(database_url: str) -> Config:
    """Build an Alembic config pointed at the packaged migrations and the given URL.

    :param database_url: the target database URL.
    :return: a configured :class:`alembic.config.Config`.
    """
    repo_root = Path(__file__).resolve().parents[3]
    cfg = Config(str(repo_root / "alembic.ini"))
    cfg.set_main_option("script_location", str(repo_root / "migrations"))
    cfg.set_main_option("sqlalchemy.url", database_url)
    return cfg


def run_migrate(database_url: str) -> bool:
    """Upgrade the configured database to the canonical schema.

    :param database_url: the database URL to migrate.
    :return: ``True`` on success, ``False`` on a handled failure.
    """
    try:
        command.upgrade(_alembic_config(database_url), "head")
    except Exception as exc:  # noqa: BLE001 - surface plain message, log detail
        logger.exception("migration failed: %s", exc)
        prompts.show("Migration failed - please check the logs.")
        return False
    prompts.show("Database is up to date.")
    return True
