"""Main Typer application for pdbsearch.

Registers all subcommands and provides an interactive top-level menu
when invoked with no subcommand.

Entry point declared in pyproject.toml:
    pdbsearch = "paper_sorts.cli.app:app"
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from paper_sorts.logging_config import setup_logging

app = typer.Typer(
    name="pdbsearch",
    help="Offline paper-database searcher: search, add, update, delete, import.",
    no_args_is_help=False,
    invoke_without_command=True,
)
console = Console()
logger = logging.getLogger(__name__)

# Global state set by the app callback
_db_url: str = ""


def _get_db_url(
    database_url: str,
    config: str,
    key: str,
) -> str:
    """Resolve the database URL from CLI flags or settings.

    Args:
        database_url: Explicit URL from --database-url flag.
        config: Path to Fernet-encrypted INI config file.
        key: Path to Fernet key file.

    Returns:
        Resolved SQLAlchemy database URL string.

    Raises:
        typer.BadParameter: If no database URL can be determined.
    """
    if database_url:
        return database_url

    import os
    os.environ.setdefault("PDBSEARCH_CONFIG", config)
    os.environ.setdefault("PDBSEARCH_KEY", key)

    from paper_sorts.config import Settings
    try:
        settings = Settings()
        if settings.database_url:
            return settings.database_url
    except Exception as exc:
        raise typer.BadParameter(
            f"Could not load database URL from config: {exc}",
            param_hint="--database-url / --config / PDBSEARCH_DATABASE_URL",
        ) from exc

    raise typer.BadParameter(
        "No database URL found. Set --database-url, PDBSEARCH_DATABASE_URL env var, "
        "or provide --config and --key pointing to an encrypted config file.",
        param_hint="--database-url",
    )


@app.callback()
def main(
    ctx: typer.Context,
    database_url: Annotated[
        str,
        typer.Option(
            "--database-url",
            envvar="PDBSEARCH_DATABASE_URL",
            help="SQLAlchemy database URL (postgresql+psycopg://...)",
        ),
    ] = "",
    log_level: Annotated[
        str,
        typer.Option(
            "--log-level",
            envvar="PDBSEARCH_LOG_LEVEL",
            help="Logging level: DEBUG, INFO, WARNING, ERROR",
        ),
    ] = "INFO",
    config: Annotated[
        str,
        typer.Option(
            "--config",
            envvar="PDBSEARCH_CONFIG",
            help="Path to Fernet-encrypted INI config file",
        ),
    ] = "",
    key: Annotated[
        str,
        typer.Option(
            "--key",
            envvar="PDBSEARCH_KEY",
            help="Path to Fernet key file",
        ),
    ] = "",
) -> None:
    """pdbsearch — offline paper database CLI.

    Run without a subcommand to enter the interactive menu.
    """
    global _db_url
    setup_logging(log_level)

    if ctx.invoked_subcommand is not None:
        # Subcommand mode: resolve DB URL and store for subcommand use
        try:
            _db_url = _get_db_url(database_url, config, key)
        except typer.BadParameter:
            # Subcommands that need the URL will fail if it's empty
            _db_url = database_url
        return

    # Interactive mode: no subcommand given
    try:
        _db_url = _get_db_url(database_url, config, key)
    except typer.BadParameter as exc:
        console.print(f"[red]Configuration error: {exc}[/red]")
        raise typer.Exit(1) from exc

    _run_interactive_menu()


def _run_interactive_menu() -> None:
    """Run the four-option interactive top-level menu.

    Loops until the user chooses Quit. Each option calls the corresponding
    subcommand callback function.
    """
    from paper_sorts.cli.add import add_callback
    from paper_sorts.cli.search import search_callback
    from paper_sorts.cli.update import update_callback

    options = [
        "Search the database",
        "Add an entry",
        "Update an entry",
        "(Q)uit",
    ]

    while True:
        from paper_sorts.cli.prompts import ask_choice

        idx = ask_choice("What do you want to do?", options)
        match idx:
            case 0:
                search_callback(_db_url)
            case 1:
                add_callback(_db_url)
            case 2:
                update_callback(_db_url)
            case 3:
                console.print("Closing connection...")
                break


# ---------------------------------------------------------------------------
# Subcommand registrations
# ---------------------------------------------------------------------------


@app.command("search")
def search_cmd(
    ctx: typer.Context,
    database_url: Annotated[
        str,
        typer.Option("--database-url", envvar="PDBSEARCH_DATABASE_URL"),
    ] = "",
) -> None:
    """Interactive search by author or paper title."""
    from paper_sorts.cli.search import search_callback

    url = _db_url or database_url
    if not url:
        console.print("[red]No database URL configured.[/red]")
        raise typer.Exit(1)
    search_callback(url)


@app.command("add")
def add_cmd(
    ctx: typer.Context,
    database_url: Annotated[
        str,
        typer.Option("--database-url", envvar="PDBSEARCH_DATABASE_URL"),
    ] = "",
) -> None:
    """Interactively add a new paper to the database."""
    from paper_sorts.cli.add import add_callback

    url = _db_url or database_url
    if not url:
        console.print("[red]No database URL configured.[/red]")
        raise typer.Exit(1)
    add_callback(url)


@app.command("update")
def update_cmd(
    ctx: typer.Context,
    database_url: Annotated[
        str,
        typer.Option("--database-url", envvar="PDBSEARCH_DATABASE_URL"),
    ] = "",
) -> None:
    """Interactively update a field in an existing database entry."""
    from paper_sorts.cli.update import update_callback

    url = _db_url or database_url
    if not url:
        console.print("[red]No database URL configured.[/red]")
        raise typer.Exit(1)
    update_callback(url)


@app.command("delete")
def delete_cmd(
    ctx: typer.Context,
    database_url: Annotated[
        str,
        typer.Option("--database-url", envvar="PDBSEARCH_DATABASE_URL"),
    ] = "",
) -> None:
    """Search for a paper by title and delete it after confirmation."""
    from paper_sorts.cli.delete import delete_callback

    url = _db_url or database_url
    if not url:
        console.print("[red]No database URL configured.[/red]")
        raise typer.Exit(1)
    delete_callback(url)


@app.command("import")
def import_cmd(
    ctx: typer.Context,
    tex: Annotated[
        Path,
        typer.Option("--tex", help="Path to the LaTeX literature overview file"),
    ],
    bib: Annotated[
        Path,
        typer.Option("--bib", help="Path to the BibTeX bibliography file"),
    ],
    database_url: Annotated[
        str,
        typer.Option("--database-url", envvar="PDBSEARCH_DATABASE_URL"),
    ] = "",
) -> None:
    """Bulk-import papers from a .tex and .bib file pair."""
    from paper_sorts.cli.importer import import_callback

    url = _db_url or database_url
    if not url:
        console.print("[red]No database URL configured.[/red]")
        raise typer.Exit(1)
    import_callback(url, tex, bib)


@app.command("migrate")
def migrate_cmd(
    ctx: typer.Context,
    database_url: Annotated[
        str,
        typer.Option("--database-url", envvar="PDBSEARCH_DATABASE_URL"),
    ] = "",
) -> None:
    """Apply all Alembic migrations to the configured database."""
    from paper_sorts.cli.migrate import migrate_callback

    url = _db_url or database_url
    if not url:
        console.print("[red]No database URL configured.[/red]")
        raise typer.Exit(1)
    migrate_callback(url)
