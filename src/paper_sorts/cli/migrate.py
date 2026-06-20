"""Migrate subcommand for pdbsearch (admin-only, not in interactive menu).

Registered as ``pdbsearch migrate`` in :mod:`paper_sorts.cli.app`.
Runs Alembic migrations to upgrade the database schema.
"""

from __future__ import annotations

import logging
from typing import Annotated

import typer

logger = logging.getLogger(__name__)

app = typer.Typer(help="Run database schema migrations (admin operation).")


@app.callback(invoke_without_command=True)
def migrate_cmd(
    ctx: typer.Context,
    revision: Annotated[
        str,
        typer.Option("--revision", help="Alembic revision target (default: head)"),
    ] = "head",
) -> None:
    """Upgrade the database schema to the specified Alembic revision.

    Idempotent — safe to run multiple times.  Handles both the modern
    (``bibtex_id``) and legacy (``bibtext_id``) schema variants.

    :param ctx: Typer context carrying the database URL.
    :param revision: Target Alembic revision (default ``"head"``).
    """
    database_url = ctx.obj["database_url"]

    try:
        from alembic import command
        from alembic.config import Config as AlembicConfig

        alembic_cfg = AlembicConfig("alembic.ini")
        alembic_cfg.set_main_option("sqlalchemy.url", database_url)
        command.upgrade(alembic_cfg, revision)
        print(f"Migrations applied successfully (target: {revision!r}).")
    except Exception as exc:
        logger.error("Migration failed: %s", exc)
        print(f"Migration failed — {exc}")
        raise typer.Exit(1) from exc
