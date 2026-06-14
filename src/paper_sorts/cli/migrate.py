"""Migrate subcommand for pdbsearch CLI.

Applies all pending Alembic migrations to the target database.
This is an admin/scripted subcommand — it does NOT appear in the
interactive top-level menu.
"""

from __future__ import annotations

import logging
from pathlib import Path

from rich.console import Console

logger = logging.getLogger(__name__)
console = Console()


def migrate_callback(db_url: str) -> None:
    """Apply all Alembic migrations up to head.

    Runs ``alembic upgrade head`` against the configured database URL.
    The migration is idempotent — safe to run multiple times.

    Args:
        db_url: SQLAlchemy-compatible database URL from the app callback.

    Raises:
        SystemExit: On Alembic failure (error is printed before exit).
    """
    import alembic.command
    import alembic.config

    # Locate alembic.ini relative to this package's project root
    project_root = Path(__file__).parent.parent.parent.parent
    alembic_ini = project_root / "alembic.ini"

    if not alembic_ini.exists():
        console.print(f"[red]alembic.ini not found at {alembic_ini}[/red]")
        raise SystemExit(1)

    cfg = alembic.config.Config(str(alembic_ini))
    cfg.set_main_option("sqlalchemy.url", db_url)

    try:
        alembic.command.upgrade(cfg, "head")
        console.print("[green]Migration complete.[/green]")
        logger.info("Alembic upgrade head completed on %r", db_url)
    except Exception as exc:
        console.print(f"[red]Migration failed — {exc}[/red]")
        logger.error("Migration failed: %s", exc)
        raise SystemExit(1) from exc
