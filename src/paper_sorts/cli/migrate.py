"""Typer `migrate` subcommand for paper_sorts.

Applies pending Alembic database migrations (upgrade head).
This is a subcommand-only operation — NOT in the four-option interactive menu.
"""

from __future__ import annotations

import logging
import os
import sys

import typer

logger = logging.getLogger("paper_sorts.cli.migrate")

app = typer.Typer()


@app.command("migrate")
def migrate_command(
    ctx: typer.Context,
    database_url: str | None = typer.Option(
        None,
        "--database-url",
        help="PostgreSQL DSN (overrides all other config sources)",
    ),
) -> None:
    """Apply pending Alembic database migrations (upgrade head).

    Safe to run on an already-migrated database (idempotent).
    Handles both legacy schema variants (bibtex_id and bibtext_id).
    """
    from paper_sorts.config import Settings

    # Resolve database URL
    if database_url:
        url = database_url
    else:
        try:
            settings: Settings = ctx.obj if isinstance(ctx.obj, Settings) else Settings()
            url = settings.get_database_url()
        except (ValueError, AttributeError):
            # ctx.obj is an engine in interactive mode; construct settings fresh
            try:
                url = Settings().get_database_url()
            except ValueError as exc:
                print(f"Cannot determine database URL: {exc}")
                sys.exit(1)

    # Set env var for Alembic env.py to pick up
    os.environ["PDBSEARCH_DATABASE_URL"] = url

    try:
        from alembic import command as alembic_cmd
        from alembic.config import Config

        # Find alembic.ini relative to this file's package root
        import importlib.resources as pkg_resources
        from pathlib import Path

        # Locate alembic.ini at project root (two levels up from src/paper_sorts/cli/)
        cli_dir = Path(__file__).parent
        project_root = cli_dir.parent.parent.parent
        alembic_ini = project_root / "alembic.ini"

        if not alembic_ini.exists():
            print(f"alembic.ini not found at {alembic_ini}")
            sys.exit(1)

        alembic_cfg = Config(str(alembic_ini))
        print(f"Applying migrations to: {url.split('@')[-1] if '@' in url else url}")
        alembic_cmd.upgrade(alembic_cfg, "head")
        print("Migrations applied successfully.")
        logger.info("Alembic upgrade head completed")
    except Exception as exc:  # noqa: BLE001
        print(f"Migration failed: {exc}")
        logger.error("Migration failed: %s", exc)
        sys.exit(1)
