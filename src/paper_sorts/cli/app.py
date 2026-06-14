"""Typer CLI application entry point for paper_sorts.

When invoked with no subcommand, drops into a 4-option interactive menu
(Search / Add / Update / Quit). Subcommands (search, add, update, delete,
import, migrate) can be called directly without the menu.

Logging is configured at startup via configure_logging().
"""

from __future__ import annotations

import logging
from typing import Annotated

import typer

from paper_sorts.config import Settings
from paper_sorts.logging_config import configure_logging

app = typer.Typer(
    name="pdbsearch",
    help="Off-line paper-database searcher.",
    no_args_is_help=False,
    invoke_without_command=True,
)

# Register subcommands
from paper_sorts.cli import (  # noqa: E402
    add_cmd,
    delete_cmd,
    import_cmd,
    migrate_cmd,
    search_cmd,
    update_cmd,
)

app.command("search")(search_cmd.search_cmd)
app.command("add")(add_cmd.add_cmd)
app.command("update")(update_cmd.update_cmd)
app.command("delete")(delete_cmd.delete_cmd)
app.command("import")(import_cmd.import_cmd)
app.command("migrate")(migrate_cmd.migrate_cmd)

logger = logging.getLogger(__name__)


def _get_settings(
    database_url: str | None,
    config: str | None,
    key: str | None,
    log_level: str,
) -> Settings:
    """Build Settings from CLI arguments.

    :param database_url: optional explicit DSN (overrides all other sources)
    :param config: optional path to Fernet-encrypted INI file
    :param key: optional path to decryption key file
    :param log_level: logging level string
    :return: Settings instance
    """
    init_kwargs: dict[str, object] = {"log_level": log_level}
    if database_url:
        init_kwargs["database_url"] = database_url
    if config:
        init_kwargs["config_file"] = config
    if key:
        init_kwargs["key_file"] = key
    return Settings(**init_kwargs)  # type: ignore[arg-type]


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    database_url: Annotated[
        str | None,
        typer.Option("--database-url", envvar="PDBSEARCH_DATABASE_URL", help="PostgreSQL DSN"),
    ] = None,
    config: Annotated[
        str | None,
        typer.Option("--config", "-c", help="Fernet-encrypted INI config file"),
    ] = None,
    key: Annotated[
        str | None,
        typer.Option("--key", "-k", help="Fernet decryption key file"),
    ] = None,
    log_level: Annotated[
        str,
        typer.Option("--log-level", envvar="PDBSEARCH_LOG_LEVEL", help="Logging level"),
    ] = "INFO",
) -> None:
    """paper_sorts: manage your local publication database.

    Run without arguments for interactive mode, or use a subcommand directly.

    :param ctx: Typer context (used to detect whether a subcommand was given)
    :param database_url: optional PostgreSQL DSN
    :param config: path to Fernet-encrypted config file
    :param key: path to Fernet key file
    :param log_level: logging level (DEBUG/INFO/WARNING/ERROR)
    """
    configure_logging(log_level)
    settings = _get_settings(database_url, config, key, log_level)
    ctx.ensure_object(dict)
    ctx.obj["settings"] = settings

    if ctx.invoked_subcommand is None:
        _interactive_menu(settings)


def _interactive_menu(settings: Settings) -> None:
    """Run the 4-option interactive main menu loop.

    Menu options: 1) Search  2) Add  3) Update  4) Quit.
    Exits cleanly on option 4 or 'q'.

    :param settings: application settings with database URL
    """
    from paper_sorts.cli.add_cmd import run_add
    from paper_sorts.cli.prompts import ask_choice
    from paper_sorts.cli.search_cmd import run_search
    from paper_sorts.cli.update_cmd import run_update

    print("Welcome to paper_sorts. Connecting to the database...")
    db_url = settings.get_database_url()

    menu_options = [
        "Search the database",
        "Add an entry",
        "Update an entry",
    ]
    while True:
        choice = ask_choice("What do you want to do?", menu_options, allow_quit=True)
        if choice is None or choice == "Quit / abort":
            print("Closing connection. Goodbye.")
            break
        elif choice == "Search the database":
            run_search(db_url)
        elif choice == "Add an entry":
            run_add(db_url)
        elif choice == "Update an entry":
            run_update(db_url)


def run() -> None:
    """Entry point called by the pdbsearch script.

    Defined separately so that pyproject.toml can reference it and
    so that Typer does not intercept the argv during module import.
    """
    app()
