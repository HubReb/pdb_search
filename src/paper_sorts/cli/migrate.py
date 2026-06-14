"""Migrate subcommand for paper_sorts CLI.

Applies all pending Alembic migrations via the Python API.
The command is idempotent — safe to run multiple times.
"""

from __future__ import annotations

import logging

import typer
from rich.console import Console

logger = logging.getLogger(__name__)
app = typer.Typer(help="Apply all pending database migrations.")
_console = Console()


@app.callback(invoke_without_command=True)
def migrate_callback(ctx: typer.Context) -> None:
    """Run migrations when invoked as subcommand.

    Args:
        ctx: Typer context.
    """
    if ctx.invoked_subcommand is None:
        from paper_sorts.cli.app import get_database_url

        run_migrate(get_database_url())


def run_migrate(database_url: str) -> None:
    """Apply all pending Alembic migrations.

    Args:
        database_url: SQLAlchemy connection string.
    """
    import os

    try:
        from alembic import command
        from alembic.config import Config

        os.environ["PDBSEARCH_DATABASE_URL"] = database_url
        alembic_cfg = Config("alembic.ini")
        alembic_cfg.set_main_option("sqlalchemy.url", database_url)
        command.upgrade(alembic_cfg, "head")
        _console.print("[green]Migration complete.[/green]")
    except Exception as exc:
        logger.exception("Migration failed")
        _console.print(f"[red]Migration failed: {exc}[/red]")
        raise typer.Exit(1) from exc
