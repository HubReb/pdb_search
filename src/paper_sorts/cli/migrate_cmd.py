"""migrate subcommand for pdbsearch CLI.

Provides `pdbsearch migrate` for applying Alembic schema migrations.
Admin-only operation; not part of the interactive 4-option menu.
"""

from __future__ import annotations

import logging
import os
from typing import Annotated

import typer

logger = logging.getLogger(__name__)

app = typer.Typer()


@app.command("migrate")
def migrate_cmd(
    ctx: typer.Context,
    target: Annotated[
        str,
        typer.Option("--target", help="Alembic revision to migrate to (default: head)"),
    ] = "head",
) -> None:
    """Apply Alembic schema migrations to the database.

    Idempotent: safe to run on an already-migrated database.
    Handles both bibtex_id and bibtext_id (legacy typo) schema variants.

    :param ctx: Typer context carrying settings from the app callback
    :param target: Alembic revision string; default is 'head' (latest)
    """
    try:
        from alembic import command
        from alembic.config import Config
    except ImportError as exc:
        print("Alembic is not installed. Run `uv sync --all-extras`.")
        logger.error("Alembic import failed: %s", exc)
        raise typer.Exit(1) from exc

    settings = ctx.obj["settings"] if ctx.obj else None
    database_url: str
    if settings is not None:
        database_url = settings.get_database_url()
    else:
        raise typer.BadParameter("No database URL configured.")

    # Set env var so Alembic's env.py picks it up
    os.environ["PDBSEARCH_DATABASE_URL"] = database_url

    alembic_cfg = Config("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", database_url)

    try:
        if target == "head":
            command.upgrade(alembic_cfg, "head")
        else:
            command.upgrade(alembic_cfg, target)

        # Report current revision
        from alembic.runtime.migration import MigrationContext
        from sqlalchemy import create_engine

        engine = create_engine(database_url)
        with engine.connect() as conn:
            mig_ctx = MigrationContext.configure(conn)
            current = mig_ctx.get_current_revision()
        print(f"Migration complete. Current revision: {current}")
    except Exception as exc:
        logger.error("Migration failed: %s", exc)
        print(f"Migration failed: {exc}")
        raise typer.Exit(1) from exc
