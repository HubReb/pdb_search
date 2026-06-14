"""Migrate subcommand for paper_sorts CLI.

Implements ``pdbsearch migrate``: runs Alembic migrations to upgrade the
personal database schema.  This is an admin-only subcommand — it is NOT
shown in the interactive top-level menu.
"""

from __future__ import annotations

import logging

import typer
from alembic import command as alembic_command
from alembic.config import Config as AlembicConfig
from rich.console import Console

logger = logging.getLogger(__name__)
console = Console()

app = typer.Typer(help="Upgrade the database schema (admin).")


@app.callback(invoke_without_command=True)
def migrate_cmd(
    ctx: typer.Context,
    revision: str = typer.Option("head", help="Alembic revision target (default: head)."),
) -> None:
    """Run Alembic migrations up to *revision* (default: ``head``).

    Uses the database URL from the application context set up by
    ``pdbsearch``'s top-level callback.

    :param ctx: Typer context (must carry ``engine`` in ``ctx.obj``).
    :param revision: Alembic revision target string (e.g. ``"head"``, ``"001"``).
    """
    if ctx.invoked_subcommand is not None:
        return
    engine = ctx.obj["engine"]

    alembic_cfg = AlembicConfig("alembic.ini")
    alembic_cfg.attributes["connection"] = engine.connect()

    try:
        with alembic_cfg.attributes["connection"] as conn:
            alembic_cfg.attributes["connection"] = conn
            alembic_command.upgrade(alembic_cfg, revision)
        console.print(f"[green]Database migrated to revision {revision!r}.[/green]")
        logger.info("Migrated database to revision %r", revision)
    except Exception as exc:
        console.print(
            f"[red]Migration failed: {exc}\nCheck logs for details.[/red]"
        )
        logger.exception("Migration failed: %s", exc)
        raise typer.Exit(code=1) from exc
