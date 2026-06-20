"""Main Typer application entry point for pdbsearch.

When invoked with no subcommand, presents an interactive four-option menu
(Search / Add / Update / Delete) and loops until the user quits.

Subcommands ``migrate`` and ``import`` are registered but intentionally absent
from the interactive menu — they are admin/scripted operations.

Entry point is declared in ``pyproject.toml``::

    [project.scripts]
    pdbsearch = "paper_sorts.cli.app:app"
"""

from __future__ import annotations

import logging

import typer

from paper_sorts.cli import add, delete, importer, migrate, search, update
from paper_sorts.logging_config import configure_logging

logger = logging.getLogger(__name__)

app = typer.Typer(
    name="pdbsearch",
    help="Off-line paper database searcher.",
    no_args_is_help=False,
    invoke_without_command=True,
)

# Register subcommands
app.add_typer(search.app, name="search")
app.add_typer(add.app, name="add")
app.add_typer(update.app, name="update")
app.add_typer(delete.app, name="delete")
app.add_typer(migrate.app, name="migrate")
app.add_typer(importer.app, name="import")


def _get_database_url(
    database_url: str | None,
    config_file: str | None,
    key_file: str | None,
) -> str | None:
    """Resolve the database URL from explicit arg or Settings.

    :param database_url: Explicit DSN (may be ``None``).
    :param config_file: Path to encrypted INI (may be ``None``).
    :param key_file: Path to Fernet key file (may be ``None``).
    :returns: Resolved database URL string, or ``None`` if not configured.
    """
    if database_url:
        return str(database_url)
    try:
        from paper_sorts.config import Settings

        settings = Settings(config_file=config_file, key_file=key_file)
        if settings.database_url:
            return str(settings.database_url)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not load settings: %s", exc)
    return None


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    database_url: str = typer.Option(
        None, "--database-url", envvar="PDBSEARCH_DATABASE_URL", help="PostgreSQL DSN"
    ),
    log_level: str = typer.Option(
        "INFO", "--log-level", envvar="PDBSEARCH_LOG_LEVEL", help="Logging level"
    ),
    config_file: str = typer.Option(
        None, "--config", envvar="PDBSEARCH_CONFIG_FILE", help="Encrypted INI config path"
    ),
    key_file: str = typer.Option(
        None, "--key", envvar="PDBSEARCH_KEY_FILE", help="Fernet key file path"
    ),
) -> None:
    """Off-line paper database searcher.

    Run without a subcommand to enter the interactive menu.
    """
    configure_logging(log_level)

    if ctx.invoked_subcommand is not None:
        # A subcommand is handling the request — store resolved URL in context.
        ctx.ensure_object(dict)
        ctx.obj = {
            "database_url": _get_database_url(database_url, config_file, key_file),
        }
        return

    # Interactive mode: four-option menu loop
    db_url = _get_database_url(database_url, config_file, key_file)
    if not db_url:
        typer.echo(
            "Error: database URL not configured.\n"
            "Set PDBSEARCH_DATABASE_URL or use --database-url.",
            err=True,
        )
        raise typer.Exit(1)

    from paper_sorts.cli.prompts import ask_choice

    print("Welcome to pdbsearch!")
    while True:
        options = ["Search", "Add", "Update", "Delete", "Quit"]
        choice = ask_choice(options, "What would you like to do?")
        if choice == 1:
            search.run_search(db_url)
        elif choice == 2:
            add.run_add(db_url)
        elif choice == 3:
            update.run_update(db_url)
        elif choice == 4:
            delete.run_delete(db_url)
        elif choice == 5:
            print("Goodbye!")
            break
