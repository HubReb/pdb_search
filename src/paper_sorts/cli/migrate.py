"""The ``migrate`` command: converge a database onto the canonical schema."""

from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config

from paper_sorts.cli import prompts

_PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _alembic_config(database_url: str) -> Config:
    """Build an Alembic config pointed at the project's migrations.

    :param database_url: the database URL to migrate.
    :returns: a configured Alembic :class:`~alembic.config.Config`.
    """
    cfg = Config(str(_PROJECT_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(_PROJECT_ROOT / "migrations"))
    cfg.set_main_option("sqlalchemy.url", database_url)
    return cfg


def run_migrate(database_url: str) -> None:
    """Run ``alembic upgrade head`` against the configured database.

    Converges either historical schema (``bibtex_id`` or the legacy
    ``bibtext_id`` typo) onto canonical, idempotently and with zero data loss.

    :param database_url: the database URL to migrate.
    """
    if not database_url:
        prompts.info("No database URL configured; cannot migrate.")
        return
    command.upgrade(_alembic_config(database_url), "head")
    prompts.info("Migration complete.")
