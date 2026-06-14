"""Migrate subcommand for paper_sorts CLI.

Admin-only; not exposed in the interactive top-level menu.
"""

from __future__ import annotations

import logging
from pathlib import Path

import typer
from alembic import command as alembic_command
from alembic.config import Config as AlembicConfig

log = logging.getLogger(__name__)

app = typer.Typer(help="Run Alembic database migrations.")


@app.command("migrate")
def migrate(
    revision: str = typer.Option("head", "--revision", help="Target revision (default: head)."),
    database_url: str = typer.Option("", "--database-url", envvar="PDBSEARCH_DATABASE_URL"),
) -> None:
    """Run pending Alembic migrations up to REVISION.

    :param revision: target Alembic revision label (default ``head``).
    :param database_url: SQLAlchemy database URL.
    """
    if not database_url:
        typer.echo("Error: database URL is required. Set --database-url or PDBSEARCH_DATABASE_URL.")
        raise typer.Exit(code=1)

    # Locate alembic.ini relative to this file's package root.
    alembic_ini = Path(__file__).parent.parent.parent.parent.parent / "alembic.ini"
    if not alembic_ini.exists():
        # Fallback: search upward from cwd
        alembic_ini = Path.cwd() / "alembic.ini"

    if not alembic_ini.exists():
        typer.echo(f"Could not find alembic.ini (searched {alembic_ini}).")
        raise typer.Exit(code=1)

    cfg = AlembicConfig(str(alembic_ini))
    cfg.set_main_option("sqlalchemy.url", database_url)

    try:
        alembic_command.upgrade(cfg, revision)
        typer.echo(f"Migration complete (target: {revision}).")
        log.info("Alembic migration complete (target=%s).", revision)
    except Exception as exc:
        typer.echo(f"Migration failed: {exc}")
        log.error("Migration failed: %s", exc)
        raise typer.Exit(code=1) from exc
