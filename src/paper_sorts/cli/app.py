"""Main Typer application and interactive top-level menu for paper_sorts.

Entry point declared in ``pyproject.toml``::

    [project.scripts]
    pdbsearch = "paper_sorts.cli.app:app"

When invoked with no subcommand, drops into the four-option interactive menu.
``migrate`` and ``import`` are subcommand-only (admin operations).
"""

from __future__ import annotations

import logging

import typer

from paper_sorts import __version__, logging_config
from paper_sorts.cli import add as add_module
from paper_sorts.cli import delete as delete_module
from paper_sorts.cli import importer as importer_module
from paper_sorts.cli import migrate as migrate_module
from paper_sorts.cli import search as search_module
from paper_sorts.cli import update as update_module
from paper_sorts.cli.prompts import ask_choice
from paper_sorts.config import Settings

log = logging.getLogger(__name__)

app = typer.Typer(
    name="pdbsearch",
    help="Offline personal paper-database searcher.",
    invoke_without_command=True,
    no_args_is_help=False,
)

# Register subcommands
app.add_typer(importer_module.app, name="import")
app.add_typer(migrate_module.app, name="migrate")


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"pdbsearch {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    ctx: typer.Context,
    database_url: str | None = typer.Option(
        None, "--database-url", envvar="PDBSEARCH_DATABASE_URL",
        help="PostgreSQL DSN (overrides all config sources)."
    ),
    log_level: str | None = typer.Option(
        None, "--log-level", envvar="PDBSEARCH_LOG_LEVEL",
        help="Logging level: DEBUG, INFO, WARNING, ERROR."
    ),
    config: str | None = typer.Option(
        None, "--config", "-c",
        help="Path to encrypted config file."
    ),
    key: str | None = typer.Option(
        None, "--key", "-k",
        help="Path to Fernet key file."
    ),
    version: bool | None = typer.Option(
        None, "--version", callback=_version_callback, is_eager=True,
        help="Show version and exit."
    ),
) -> None:
    """Paper database searcher — interactive mode or direct subcommands."""
    # Build settings, applying CLI overrides.
    overrides: dict[str, str] = {}
    if database_url:
        overrides["database_url"] = database_url
    if log_level:
        overrides["log_level"] = log_level
    if config:
        overrides["config"] = config
    if key:
        overrides["key"] = key

    try:
        settings = Settings(**overrides)
    except Exception as exc:
        typer.echo(f"Configuration error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    # Set up logging once.
    logging_config.setup(log_level=settings.get_log_level_int())

    # Store resolved database_url in context for subcommands.
    ctx.ensure_object(dict)
    ctx.obj["database_url"] = settings.database_url
    ctx.obj["settings"] = settings

    # If a subcommand is being invoked, do not enter interactive mode.
    if ctx.invoked_subcommand is not None:
        return

    # --- Interactive top-level menu ---
    if not settings.database_url:
        typer.echo(
            "No database URL configured. Set PDBSEARCH_DATABASE_URL or use --database-url.",
            err=True,
        )
        raise typer.Exit(code=1)

    typer.echo("Welcome! Connected to the database.")
    while True:
        choice = ask_choice(
            "What do you want to do?",
            ["Search the database", "Add an entry", "Update an entry"],
        )
        if choice is None:
            typer.echo("Closing connection...")
            break
        match choice:
            case 1:
                try:
                    search_module._run_search(settings.database_url)
                except Exception as exc:
                    typer.echo(f"Search failed: {exc}")
                    log.error("Search error: %s", exc)
            case 2:
                try:
                    add_module._run_add(settings.database_url)
                except Exception as exc:
                    typer.echo(f"Add failed: {exc}")
                    log.error("Add error: %s", exc)
            case 3:
                try:
                    delete_choice = ask_choice(
                        "Update or delete?",
                        ["Update an entry", "Delete a paper"],
                    )
                    if delete_choice == 1:
                        update_module._run_update(settings.database_url)
                    elif delete_choice == 2:
                        delete_module._run_delete(settings.database_url)
                except Exception as exc:
                    typer.echo(f"Operation failed: {exc}")
                    log.error("Operation error: %s", exc)
