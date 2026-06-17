"""Migrate subcommand: apply Alembic migrations against the configured database."""

from __future__ import annotations

from rich.console import Console
from sqlalchemy import Engine

from paper_sorts.logging_config import get_logger

console = Console()
logger = get_logger()


def run_migrate(engine: Engine) -> bool:
    """Upgrade the database to the latest Alembic revision (idempotent).

    Converges a legacy ``bibtext_id`` schema onto canonical ``bibtex_id`` with
    zero data loss (FR-011 / US4). Safe to rerun.

    :param engine: the engine bound to the configured database.
    :returns: ``True`` on success, ``False`` on a handled failure.
    """
    from alembic import command
    from alembic.config import Config

    try:
        cfg = Config("alembic.ini")
        cfg.attributes["sqlalchemy.url"] = str(engine.url)
        command.upgrade(cfg, "head")
    except Exception as exc:  # noqa: BLE001 - surface as a plain message
        logger.error("migration failed: %s", exc)
        console.print("Migration failed - please check logs.")
        return False
    console.print("Database is up to date.")
    return True
