"""``pdbsearch migrate`` — bring the personal database up to schema head.

Reachable as a Typer subcommand only — *not* from the top-level menu —
because schema migration is an admin/setup operation; the same
UX-surface-preservation reasoning that keeps delete out of the menu
(per ``contracts/cli-commands.md`` § "Why only four options") applies.

The command:

* Reports row counts for the four tables before any work runs.
* If the current schema is already at ``head``, prints
  ``"Schema is at head (<rev>). No migrations to apply."`` and exits.
* Otherwise runs ``alembic upgrade head`` against the configured
  database via the pre-attached-connection pattern that
  ``migrations/env.py`` supports, and prints the post-migration row
  counts.
"""

from __future__ import annotations

from pathlib import Path
from typing import cast

import typer
from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker


def migrate(ctx: typer.Context) -> None:
    """Apply pending migrations and report row counts before and after."""
    factory = cast("sessionmaker[Session]", ctx.obj)
    engine = cast("Engine", factory.kw["bind"])

    cfg = _alembic_config()
    head = _head_revision(cfg)

    with engine.connect() as conn:
        current = MigrationContext.configure(conn).get_current_revision()

    if current == head:
        print(f"Schema is at head ({head}). No migrations to apply.")
        _print_counts(engine, label="Tables")
        return

    counts_before = _row_counts(engine)
    print("Running Alembic upgrade...")
    with engine.begin() as conn:
        cfg.attributes["connection"] = conn
        command.upgrade(cfg, "head")
    counts_after = _row_counts(engine)

    parts = [
        f"{table}={counts_after[table]}"
        for table in ("papers", "authors_id", "bib", "authors_papers")
    ]
    if counts_before != counts_after:
        for table in ("papers", "authors_id", "bib", "authors_papers"):
            if counts_before[table] != counts_after[table]:
                print(f"  {table}: {counts_before[table]} -> {counts_after[table]}")
    print(f"Tables: {', '.join(parts)}")
    print(f"Schema is at head ({head}).")


def _alembic_config() -> Config:
    """Build an alembic Config rooted at the repo's ``alembic.ini``.

    The path lookup walks up from this module so the migrate command works
    when invoked from any CWD inside (or outside) the repository.
    """
    return Config(str(Path(__file__).resolve().parents[3] / "alembic.ini"))


def _head_revision(cfg: Config) -> str:
    """Return the migration tree's head revision id."""
    head = ScriptDirectory.from_config(cfg).get_current_head()
    if head is None:
        msg = "no migration head found"
        raise RuntimeError(msg)
    return head


def _row_counts(engine: Engine) -> dict[str, int]:
    """Return ``{table: count}`` for the four schema tables, treating absent ones as 0."""
    counts: dict[str, int] = {}
    with engine.connect() as conn:
        for table in ("papers", "authors_id", "bib", "authors_papers"):
            try:
                result = conn.execute(text(f"SELECT count(*) FROM {table}"))  # noqa: S608
                counts[table] = int(result.scalar_one())
            except Exception:  # pre-migration tables may not exist
                counts[table] = 0
    return counts


def _print_counts(engine: Engine, *, label: str) -> None:
    """Print one-line row-count summary."""
    counts = _row_counts(engine)
    parts = [
        f"{table}={counts[table]}" for table in ("papers", "authors_id", "bib", "authors_papers")
    ]
    print(f"{label}: {', '.join(parts)}")
