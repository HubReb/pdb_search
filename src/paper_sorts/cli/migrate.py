"""Schema ``migrate`` flow (presentation layer, subcommand-only).

Runs Alembic ``upgrade head`` against the configured database. The migration is
idempotent and converges either historical schema to canonical.
"""

from __future__ import annotations

import logging
from pathlib import Path

from alembic import command
from alembic.config import Config

_logger = logging.getLogger(__name__)


def _alembic_config(database_url: str) -> Config:
    root = Path(__file__).resolve().parents[3]
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "migrations"))
    cfg.set_main_option("sqlalchemy.url", database_url)
    return cfg


def run_migrate(database_url: str) -> None:
    """Upgrade the database at ``database_url`` to the head revision.

    :param database_url: the SQLAlchemy URL of the database to migrate.
    """
    _logger.info("running alembic upgrade head")
    command.upgrade(_alembic_config(database_url), "head")
