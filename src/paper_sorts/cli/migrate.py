"""``migrate`` subcommand: bring a personal database to the canonical schema.

Idempotent and zero-loss. Three cases, all converging on the canonical schema at
the Alembic head with row counts unchanged:

1. **Already version-tracked** — run ``upgrade head`` (a no-op once at head).
2. **Untracked legacy/canonical tables** (a personal DB from the pre-Alembic
   tool) — rename any legacy ``bibtext_id`` (sic) columns to ``bibtex_id`` via
   revision 002's logic, then ``stamp head`` so the existing rows are adopted
   without recreating tables.
3. **Empty database** — run ``upgrade head`` to create the schema from scratch.

Admin/scripted — deliberately absent from the interactive menu.
"""

from __future__ import annotations

import logging

from alembic import command
from alembic.config import Config
from rich.console import Console
from sqlalchemy import inspect, text

from paper_sorts.db.session import create_db_engine

_logger = logging.getLogger(__name__)
_console = Console()

_COUNTED_TABLES = ("papers", "authors_id", "authors_papers", "bib")


def _alembic_config(database_url: str) -> Config:
    """Build an Alembic config pointed at a specific database URL.

    :param database_url: the target database URL.
    :return: a configured :class:`alembic.config.Config`.
    """
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", database_url)
    return cfg


def _row_counts(database_url: str) -> dict[str, int]:
    """Return per-table row counts for the canonical tables that exist.

    :param database_url: the target database URL.
    :return: mapping of table name to row count (absent tables omitted).
    """
    engine = create_db_engine(database_url)
    counts: dict[str, int] = {}
    try:
        existing = set(inspect(engine).get_table_names())
        with engine.connect() as conn:
            for table in _COUNTED_TABLES:
                if table in existing:
                    counts[table] = conn.execute(
                        text(f"SELECT count(*) FROM {table}")  # noqa: S608 - fixed names
                    ).scalar_one()
    finally:
        engine.dispose()
    return counts


def _converge_untracked_legacy(database_url: str) -> None:
    """Rename legacy ``bibtext_id`` (sic) columns in place, then stamp head.

    :param database_url: the target database URL.
    """
    engine = create_db_engine(database_url)
    try:
        with engine.begin() as conn:
            cols = {
                (row[0], row[1])
                for row in conn.execute(
                    text(
                        "SELECT table_name, column_name FROM information_schema.columns "
                        "WHERE table_name IN ('papers', 'bib')"
                    )
                )
            }
            for table in ("papers", "bib"):
                if (table, "bibtext_id") in cols and (table, "bibtex_id") not in cols:
                    conn.execute(text(f"ALTER TABLE {table} RENAME COLUMN bibtext_id TO bibtex_id"))
            if ("bib", "bibtext") in cols and ("bib", "bibtex") not in cols:
                conn.execute(text("ALTER TABLE bib RENAME COLUMN bibtext TO bibtex"))
    finally:
        engine.dispose()
    command.stamp(_alembic_config(database_url), "head")


def run_migrate(database_url: str) -> None:
    """Upgrade the database at ``database_url`` to the canonical head schema.

    :param database_url: the target database URL.
    """
    before = _row_counts(database_url)
    engine = create_db_engine(database_url)
    try:
        existing = set(inspect(engine).get_table_names())
    finally:
        engine.dispose()

    if "alembic_version" in existing:
        command.upgrade(_alembic_config(database_url), "head")
    elif existing & set(_COUNTED_TABLES):
        _converge_untracked_legacy(database_url)
    else:
        command.upgrade(_alembic_config(database_url), "head")

    after = _row_counts(database_url)
    _logger.info("migration row counts before=%s after=%s", before, after)
    if before and before != after:
        _console.print(
            f"Warning: row counts changed during migration — before {before}, after {after}."
        )
    else:
        _console.print("Migration complete. All rows preserved.")
        for table, count in sorted(after.items()):
            _console.print(f"  {table}: {count}")
