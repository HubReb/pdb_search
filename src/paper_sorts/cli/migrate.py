"""``migrate`` subcommand: bring a personal database to the canonical schema.

Idempotent and zero-loss. Three cases, all converging on the canonical schema at
the Alembic head with row counts unchanged:

1. **Already version-tracked** — run ``upgrade head`` (a no-op once at head).
2. **Untracked legacy/canonical tables** (a personal DB from the pre-Alembic
   tool) — rename any legacy ``bibtext_id`` (sic) columns to ``bibtex_id``, then
   ``stamp head`` so the existing rows are adopted without recreating tables.
3. **Empty database** — run ``upgrade head`` to create the schema from scratch.

Admin/scripted — deliberately absent from the interactive menu. Schema
inspection and the legacy rename live in ``paper_sorts.db.maintenance`` so this
command never imports ``sqlalchemy`` (Principle I).
"""

from __future__ import annotations

import logging

from alembic import command
from alembic.config import Config
from rich.console import Console

from paper_sorts.db.maintenance import (
    COUNTED_TABLES,
    converge_legacy_columns,
    list_tables,
    row_counts,
)

_logger = logging.getLogger(__name__)
_console = Console()


def _alembic_config(database_url: str) -> Config:
    """Build an Alembic config pointed at a specific database URL.

    :param database_url: the target database URL.
    :return: a configured :class:`alembic.config.Config`.
    """
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", database_url)
    return cfg


def run_migrate(database_url: str) -> None:
    """Upgrade the database at ``database_url`` to the canonical head schema.

    :param database_url: the target database URL.
    """
    before = row_counts(database_url)
    existing = list_tables(database_url)

    if "alembic_version" in existing:
        command.upgrade(_alembic_config(database_url), "head")
    elif existing & set(COUNTED_TABLES):
        converge_legacy_columns(database_url)
        command.stamp(_alembic_config(database_url), "head")
    else:
        command.upgrade(_alembic_config(database_url), "head")

    after = row_counts(database_url)
    _logger.info("migration row counts before=%s after=%s", before, after)
    if before and before != after:
        _console.print(
            f"Warning: row counts changed during migration — before {before}, after {after}."
        )
    else:
        _console.print("Migration complete. All rows preserved.")
        for table, count in sorted(after.items()):
            _console.print(f"  {table}: {count}")
