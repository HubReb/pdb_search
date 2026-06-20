"""Migrate subcommand for pdbsearch CLI.

Runs Alembic migrations to bring the database schema up to the latest
revision.  Handles both legacy schema variants (bibtex_id / bibtext_id typo).
"""

import logging
import os
import sys

import typer

logger = logging.getLogger(__name__)

app = typer.Typer(help="Apply database schema migrations.")


@app.command()
def migrate_cmd(
    ctx: typer.Context,
    target: str = typer.Option("head", "--target", help="Alembic revision target"),
) -> None:
    """Apply Alembic migrations up to *target* revision (default: head).

    Idempotent — re-running on an already-up-to-date database is a no-op.

    :param ctx: Typer context carrying the database_url.
    :param target: Alembic revision to migrate to (default ``"head"``).
    """
    database_url = None
    if ctx.obj:
        engine = ctx.obj.get("engine")
        if engine is not None:
            database_url = str(engine.url)

    if not database_url:
        database_url = os.environ.get("PDBSEARCH_DATABASE_URL", "")

    if not database_url:
        print(
            "Error: no database URL configured. "
            "Pass --database-url or set PDBSEARCH_DATABASE_URL."
        )
        sys.exit(1)

    # Set env var so migrations/env.py can pick it up
    os.environ["PDBSEARCH_DATABASE_URL"] = database_url

    try:
        from alembic import command
        from alembic.config import Config

        alembic_cfg = Config("alembic.ini")
        print(f"Running migrations to '{target}'...")
        command.upgrade(alembic_cfg, target)
        print("Migrations complete.")
    except Exception as exc:
        logger.error("Migration failed: %s", exc)
        print(f"Migration failed: {exc}")
        sys.exit(1)
