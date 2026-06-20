"""Migrate subcommand for pdbsearch (admin/scripted only).

Runs ``alembic upgrade head`` against the configured database.  Not shown in
the interactive four-option menu — intended for first-time setup or schema
upgrades.

Usage::

    pdbsearch migrate
    pdbsearch migrate --database-url postgresql+psycopg://...
"""

from __future__ import annotations

import logging
import os
import pathlib

import typer
from alembic import command
from alembic.config import Config

logger = logging.getLogger(__name__)

app = typer.Typer(help="Run Alembic schema migrations (admin only).")


@app.callback(invoke_without_command=True)
def migrate_cmd(
    ctx: typer.Context,
    database_url: str = typer.Option(
        None, "--database-url", envvar="PDBSEARCH_DATABASE_URL", help="PostgreSQL DSN"
    ),
) -> None:
    """Apply all pending Alembic migrations (``alembic upgrade head``).

    :param database_url: PostgreSQL DSN.  Required.
    """
    if ctx.invoked_subcommand is not None:
        return
    if not database_url:
        typer.echo("Error: database URL not configured.", err=True)
        raise typer.Exit(1)

    # Set the env var so migrations/env.py can read it.
    os.environ["PDBSEARCH_DATABASE_URL"] = str(database_url)

    alembic_ini = pathlib.Path.cwd() / "alembic.ini"
    if not alembic_ini.exists():
        typer.echo(
            "Warning: alembic.ini not found in current directory. "
            "Run this command from the project root.",
            err=True,
        )
        raise typer.Exit(1)

    try:
        typer.echo("Running migrations...")
        cfg = Config(str(alembic_ini))
        command.upgrade(cfg, "head")
        typer.echo("Migrations complete.")
    except Exception as exc:
        logger.error("Migration failed: %s", exc)
        typer.echo(f"Migration failed: {exc}", err=True)
        raise typer.Exit(1) from exc
