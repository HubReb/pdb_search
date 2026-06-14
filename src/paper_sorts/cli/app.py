"""Typer CLI entry point for paper_sorts.

Registers subcommands and provides an interactive top-level menu when invoked
with no subcommand.  Calls configure_logging at startup.

Constitution Principle III: all prompts route through cli/prompts.py.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from paper_sorts.cli import add as add_cmd
from paper_sorts.cli import delete as delete_cmd
from paper_sorts.cli import importer as import_cmd
from paper_sorts.cli import migrate as migrate_cmd
from paper_sorts.cli import search as search_cmd
from paper_sorts.cli import update as update_cmd
from paper_sorts.logging_config import configure_logging

app = typer.Typer(
    name="pdbsearch",
    help="Off-line paper-database searcher.",
    no_args_is_help=False,
    invoke_without_command=True,
)

app.add_typer(search_cmd.app, name="search")
app.add_typer(add_cmd.app, name="add")
app.add_typer(update_cmd.app, name="update")
app.add_typer(delete_cmd.app, name="delete")
app.add_typer(import_cmd.app, name="import")
app.add_typer(migrate_cmd.app, name="migrate")


# Global state holder — populated by callback before subcommands run.
_database_url: str = ""


def get_database_url() -> str:
    """Return the resolved database URL set by the CLI callback.

    Returns:
        Database URL string.
    """
    return _database_url


@app.callback()
def main_callback(
    ctx: typer.Context,
    database_url: Annotated[
        str | None,
        typer.Option("--database-url", envvar="PDBSEARCH_DATABASE_URL", help="PostgreSQL connection URL"),
    ] = None,
    log_level: Annotated[
        str,
        typer.Option("--log-level", envvar="PDBSEARCH_LOG_LEVEL", help="Logging level"),
    ] = "INFO",
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Path to Fernet-encrypted INI config file"),
    ] = None,
    key: Annotated[
        Path | None,
        typer.Option("--key", help="Path to Fernet key file"),
    ] = None,
) -> None:
    """Initialise logging and resolve database URL.

    When invoked with no subcommand, drops into the interactive top-level menu.

    Args:
        ctx: Typer context (used to detect whether a subcommand was given).
        database_url: PostgreSQL connection string (overrides env/config).
        log_level: Logging level (DEBUG/INFO/WARNING/ERROR).
        config: Path to Fernet-encrypted INI config file.
        key: Path to Fernet key file.
    """
    global _database_url

    configure_logging(log_level)

    # Resolve database URL from CLI > env > encrypted config
    if database_url:
        _database_url = database_url
    elif config and key:
        try:
            from paper_sorts.config import FernetIniSettingsSource, Settings

            source = FernetIniSettingsSource(Settings, config, key)
            data = source()
            _database_url = data.get("database_url", "")
        except ValueError as exc:
            typer.echo(f"Configuration error: {exc}", err=True)
            raise typer.Exit(1) from exc
    else:
        from paper_sorts.config import Settings

        settings = Settings()
        _database_url = settings.database_url

    if ctx.invoked_subcommand is None:
        _run_interactive_menu()


def _run_interactive_menu() -> None:
    """Run the four-option top-level interactive menu."""
    from rich.console import Console

    console = Console()
    while True:
        console.print("\n[bold]paper_sorts[/bold]")
        console.print("1) Search")
        console.print("2) Add")
        console.print("3) Update")
        console.print("4) Delete")
        console.print("q) Quit")

        choice_raw = typer.prompt("Choice").strip().lower()
        if choice_raw in {"q", "quit"}:
            raise typer.Exit(0)
        try:
            choice = int(choice_raw)
        except ValueError:
            console.print("[yellow]Please enter 1–4 or q.[/yellow]")
            continue

        if choice == 1:
            search_cmd.run_search_menu(_database_url)
        elif choice == 2:
            add_cmd.run_add(_database_url)
        elif choice == 3:
            update_cmd.run_update(_database_url)
        elif choice == 4:
            delete_cmd.run_delete(_database_url)
        else:
            console.print("[yellow]Please enter 1–4 or q.[/yellow]")


def main() -> None:
    """Entry point registered in pyproject.toml as pdbsearch."""
    app()


if __name__ == "__main__":
    main()
