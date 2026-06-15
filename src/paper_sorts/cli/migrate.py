"""CLI 'migrate' subcommand for paper_sorts.

Applies all pending Alembic migrations via 'alembic upgrade head'.
Non-interactive. Idempotent (safe to run multiple times).
"""

from __future__ import annotations

import logging

import typer

logger = logging.getLogger(__name__)

app = typer.Typer()


def run_migrate(db_url: str) -> None:
    """Apply all pending Alembic migrations.

    :param db_url: SQLAlchemy-compatible database URL (overrides alembic.ini).
    """
    # Locate alembic.ini relative to this package (project root)
    from pathlib import Path

    from alembic import command
    from alembic.config import Config

    # Walk up from this file to find alembic.ini
    here = Path(__file__).resolve().parent
    ini_path: Path | None = None
    for parent in [here, *here.parents]:
        candidate = parent / "alembic.ini"
        if candidate.exists():
            ini_path = candidate
            break

    if ini_path is None:
        typer.echo("alembic.ini not found. Cannot run migrations.", err=True)
        raise typer.Exit(code=1)  # noqa: B904 — not in an except block

    alembic_cfg = Config(str(ini_path))
    alembic_cfg.set_main_option("sqlalchemy.url", db_url)

    typer.echo("Applying migrations...")
    try:
        command.upgrade(alembic_cfg, "head")
        typer.echo("All migrations applied successfully.")
    except Exception as exc:
        logger.error("Migration failed: %s", exc)
        typer.echo(f"Migration failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc


@app.command()
def migrate(ctx: typer.Context) -> None:
    """Apply all pending Alembic schema migrations."""
    db_url: str = ctx.obj["db_url"]
    run_migrate(db_url)
