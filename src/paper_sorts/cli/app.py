"""Root Typer application for paper_sorts (pdbsearch entry point).

When invoked with no subcommand, drops into the four-option interactive
top-level menu (search, add, update, quit). The migrate and import subcommands
are available as direct subcommands only — they are not part of the interactive
menu (admin/scripted operations).

All prompts route through cli/prompts.py (constitution Principle III).
Logging is configured once here at startup (constitution Principle I).
"""

from __future__ import annotations

import logging
from pathlib import Path

import typer

from paper_sorts.cli.prompts import ask_menu
from paper_sorts.logging_config import configure_logging

logger = logging.getLogger(__name__)

app = typer.Typer(
    name="pdbsearch",
    help="Off-line paper-database searcher. Run without a subcommand for interactive mode.",
    invoke_without_command=True,
)


@app.callback()
def main_callback(
    ctx: typer.Context,
    database_url: str | None = typer.Option(
        None,
        "--database-url",
        envvar="PDBSEARCH_DATABASE_URL",
        help="SQLAlchemy database URL (postgresql+psycopg://...).",
    ),
    log_level: str = typer.Option(
        "INFO",
        "--log-level",
        envvar="PDBSEARCH_LOG_LEVEL",
        help="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).",
    ),
    config_file: Path | None = typer.Option(  # noqa: B008
        None,
        "--config",
        envvar="PDBSEARCH_CONFIG_FILE",
        help="Path to Fernet-encrypted INI config file.",
    ),
    key_file: Path | None = typer.Option(  # noqa: B008
        None,
        "--key",
        envvar="PDBSEARCH_KEY_FILE",
        help="Path to Fernet decryption key file.",
    ),
) -> None:
    """paper_sorts — offline paper database searcher.

    Configure via --database-url or PDBSEARCH_DATABASE_URL env var.
    For Fernet-encrypted config use --config and --key.

    :param ctx: Typer context (carries db_url to subcommands).
    :param database_url: SQLAlchemy DB URL (overrides all other sources if set).
    :param log_level: Logging verbosity level.
    :param config_file: Path to encrypted INI config file.
    :param key_file: Path to Fernet key file.
    """
    configure_logging(log_level)
    ctx.ensure_object(dict)

    # Resolve database URL from flag or encrypted config
    resolved_url: str | None = database_url
    if resolved_url is None and config_file is not None and key_file is not None:
        try:
            from paper_sorts.config import Settings

            settings = Settings(config_file=config_file, key_file=key_file)
            resolved_url = settings.database_url
        except Exception as exc:
            logger.error("Failed to load config: %s", exc)
            typer.echo(f"Configuration error: {exc}", err=True)
            raise typer.Exit(code=1) from exc

    if resolved_url is None:
        # Try to load from environment / .env via Settings
        try:
            from paper_sorts.config import Settings

            settings = Settings()
            if settings.db_name:
                resolved_url = settings.database_url
        except Exception:
            pass

    ctx.obj["db_url"] = resolved_url or ""

    # If no subcommand was invoked, drop into the interactive menu
    if ctx.invoked_subcommand is None:
        if not ctx.obj["db_url"]:
            typer.echo(
                "No database URL configured. Set --database-url or PDBSEARCH_DATABASE_URL.",
                err=True,
            )
            raise typer.Exit(code=1)
        _interactive_menu(ctx.obj["db_url"])


def _interactive_menu(db_url: str) -> None:
    """Run the four-option top-level interactive menu.

    :param db_url: SQLAlchemy-compatible database URL.
    """
    from paper_sorts.cli.add import run_add
    from paper_sorts.cli.search import run_search
    from paper_sorts.cli.update import run_update

    typer.echo("Welcome to paper_sorts!")

    while True:
        choice_idx = ask_menu(
            "What do you want to do?",
            [
                "Search the database",
                "Add an entry",
                "Update an entry",
                "(Q)uit",
            ],
        )
        match choice_idx:
            case 0:
                run_search(db_url)
            case 1:
                run_add(db_url)
            case 2:
                run_update(db_url)
            case 3:
                typer.echo("Goodbye.")
                break


# Register subcommands
from paper_sorts.cli.add import add  # noqa: E402
from paper_sorts.cli.delete import delete  # noqa: E402
from paper_sorts.cli.importer import importer  # noqa: E402
from paper_sorts.cli.migrate import migrate  # noqa: E402
from paper_sorts.cli.search import search  # noqa: E402
from paper_sorts.cli.update import update  # noqa: E402

app.command()(search)
app.command()(add)
app.command()(update)
app.command()(delete)
app.command("import")(importer)
app.command()(migrate)


def main() -> None:
    """Entry point for the pdbsearch console script."""
    app()


if __name__ == "__main__":
    main()
