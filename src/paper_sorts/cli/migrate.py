"""Migrate subcommand for paper_sorts CLI.

Runs Alembic upgrade head against the configured database.
Not part of the interactive menu — subcommand only (admin operation).
"""

import logging
import sys

import typer

logger = logging.getLogger(__name__)

app = typer.Typer(help="Upgrade the database schema to the current revision.")


def _find_alembic_ini() -> str:
    """Locate alembic.ini by searching from the current working directory upward.

    :returns: Path to alembic.ini as a string.
    :raises FileNotFoundError: if alembic.ini cannot be found.
    """
    import os
    from pathlib import Path

    cwd = Path(os.getcwd())
    for directory in [cwd, *cwd.parents]:
        candidate = directory / "alembic.ini"
        if candidate.exists():
            return str(candidate)
    raise FileNotFoundError("alembic.ini not found in current directory or any parent")


@app.callback(invoke_without_command=True)
def migrate(ctx: typer.Context) -> None:
    """Run 'alembic upgrade head' against the configured database."""
    if ctx.resilient_parsing:
        return

    database_url: str | None = ctx.obj.get("database_url") if ctx.obj else None
    if not database_url:
        typer.echo("Error: no database URL configured.", err=True)
        sys.exit(1)
    try:
        import os

        from alembic import command
        from alembic.config import Config

        alembic_ini = _find_alembic_ini()
        alembic_cfg = Config(alembic_ini)
        alembic_cfg.set_main_option("sqlalchemy.url", database_url)

        os.environ["PDBSEARCH_DATABASE_URL"] = database_url
        command.upgrade(alembic_cfg, "head")
        typer.echo("Database schema upgraded successfully.")
    except Exception as exc:
        logger.exception("Migration failed: %s", exc)
        typer.echo(f"Migration failed: {exc}", err=True)
        sys.exit(1)
