"""Main Typer application entry point for paper_sorts.

Wires all subcommands (search, add, update, delete, migrate, import) into a
single ``pdbsearch`` CLI.  When invoked with no subcommand, drops into the
four-option interactive top-level menu (search / add / update / quit).

``migrate`` and ``import`` subcommands are intentionally absent from the
interactive menu — they are admin/scripted operations.
"""

from __future__ import annotations

import logging
from pathlib import Path

import typer
from rich.console import Console
from sqlalchemy.engine import Engine

from paper_sorts.cli import add, delete, migrate, search, update
from paper_sorts.cli.importer import import_cmd
from paper_sorts.cli.prompts import ask_choice
from paper_sorts.db.session import get_engine
from paper_sorts.logging_config import setup_logging

logger = logging.getLogger(__name__)
console = Console()

app = typer.Typer(
    name="pdbsearch",
    help="Off-line paper-database searcher.",
    invoke_without_command=True,
)

# Register subcommands
app.add_typer(search.app, name="search")
app.add_typer(add.app, name="add")
app.add_typer(update.app, name="update")
app.add_typer(delete.app, name="delete")
app.add_typer(migrate.app, name="migrate")

# Register import as a plain command (different signature from other subcommands)
app.command("import")(import_cmd)


@app.callback(invoke_without_command=True)
def main_callback(
    ctx: typer.Context,
    database_url: str | None = typer.Option(
        None,
        "--database-url",
        envvar="PDBSEARCH_DATABASE_URL",
        help="PostgreSQL DSN.",
    ),
    log_level: str = typer.Option(
        "INFO",
        "--log-level",
        envvar="PDBSEARCH_LOG_LEVEL",
        help="Logging level (DEBUG/INFO/WARNING/ERROR).",
    ),
    config: Path | None = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to Fernet-encrypted INI config.",
    ),
    key: Path | None = typer.Option(
        None,
        "--key",
        "-k",
        help="Path to Fernet key file.",
    ),
) -> None:
    """pdbsearch — off-line paper-database searcher.

    Run with no subcommand to enter interactive mode.
    """
    setup_logging(log_level)

    # Resolve database URL
    effective_url = database_url
    if not effective_url:
        # Try Fernet-encrypted config
        if config and key:
            try:
                from paper_sorts.config import Settings

                settings = Settings(config_path=config, key_path=key)
                effective_url = settings.resolve_database_url()
            except Exception as exc:
                console.print(f"[red]Config error: {exc}[/red]")
                raise typer.Exit(code=1) from exc
        else:
            # Try env / .env via Settings
            try:
                from paper_sorts.config import Settings

                settings = Settings()
                effective_url = settings.resolve_database_url()
            except ValueError:
                # No URL available — only a problem if they try to use the DB
                effective_url = ""

    engine: Engine | None = None
    if effective_url:
        try:
            engine = get_engine(effective_url)
        except Exception as exc:
            console.print(f"[red]Database connection error: {exc}[/red]")
            raise typer.Exit(code=1) from exc

    ctx.ensure_object(dict)
    ctx.obj["engine"] = engine

    # If a subcommand was given, let Typer invoke it
    if ctx.invoked_subcommand is not None:
        return

    # No subcommand → interactive menu
    if engine is None:
        console.print(
            "[red]No database URL configured. Set PDBSEARCH_DATABASE_URL or "
            "pass --database-url / --config + --key.[/red]"
        )
        raise typer.Exit(code=1)

    _interactive_loop(engine)


def _interactive_loop(engine: Engine) -> None:
    """Run the four-option interactive top-level menu until the user quits.

    :param engine: Active SQLAlchemy engine.
    """
    console.print("Welcome to pdbsearch!")
    while True:
        choice = ask_choice(
            [
                "Search the database",
                "Add an entry",
                "Update an entry",
                "(Q)uit",
            ],
            "What do you want to do",
        )
        match choice:
            case 1:
                search.run_search(engine)
            case 2:
                add.run_add(engine)
            case 3:
                update.run_update(engine)
            case 4:
                console.print("Goodbye!")
                raise typer.Exit()


def entry_point() -> None:
    """Entry point declared in pyproject.toml scripts.

    :raises SystemExit: When Typer calls ``sys.exit`` at the end.
    """
    app()
