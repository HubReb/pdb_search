"""Main Typer application for pdbsearch.

Entry point: ``pdbsearch`` (configured in ``pyproject.toml`` ``[project.scripts]``).

When invoked with **no subcommand**, drops into a 5-option interactive
top-level menu (search / add / update / delete / quit).

Subcommands ``migrate`` and ``import`` are admin/scripted operations and are
deliberately absent from the interactive menu.
"""

from __future__ import annotations

import logging
from typing import Annotated

import typer

from paper_sorts.cli import add, delete, importer, migrate, search, update
from paper_sorts.cli.prompts import ask_choice
from paper_sorts.config import Settings
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


@app.callback()
def main(
    ctx: typer.Context,
    database_url: Annotated[
        str | None,
        typer.Option("--database-url", envvar="PDBSEARCH_DATABASE_URL", help="SQLAlchemy DB URL"),
    ] = None,
    log_level: Annotated[
        str,
        typer.Option("--log-level", envvar="PDBSEARCH_LOG_LEVEL", help="Logging level"),
    ] = "INFO",
    config: Annotated[
        str | None,
        typer.Option("--config", help="Path to Fernet-encrypted INI config file"),
    ] = None,
    key: Annotated[
        str | None,
        typer.Option("--key", help="Path to Fernet decryption key file"),
    ] = None,
) -> None:
    """pdbsearch — personal paper database CLI.

    When invoked with no subcommand, presents an interactive menu.

    :param ctx: Typer context for passing objects to subcommands.
    :param database_url: SQLAlchemy connection URL.
    :param log_level: Python logging level (default INFO).
    :param config: Path to Fernet-encrypted INI config file.
    :param key: Path to Fernet key file.
    """
    configure_logging(log_level)

    # Build Settings — CLI flags override env/file sources
    init_kwargs: dict[str, object] = {"log_level": log_level}
    if database_url:
        init_kwargs["database_url"] = database_url
    if config:
        init_kwargs["config_file"] = config
    if key:
        init_kwargs["key_file"] = key

    try:
        settings = Settings(**init_kwargs)  # type: ignore[arg-type]
    except Exception as exc:
        logger.error("Configuration error: %s", exc)
        print(f"Configuration error: {exc}")
        raise typer.Exit(1) from exc

    if not settings.database_url:
        print(
            "No database URL configured. "
            "Set PDBSEARCH_DATABASE_URL or use --database-url."
        )
        raise typer.Exit(1)

    from paper_sorts.db.session import get_engine

    engine = get_engine(settings.database_url)

    ctx.ensure_object(dict)
    ctx.obj["engine"] = engine
    ctx.obj["database_url"] = settings.database_url

    if ctx.invoked_subcommand is None:
        # Interactive top-level menu
        _interactive_menu(ctx, engine)


class _SimpleCtx:
    """Minimal context stand-in for interactive menu calls to subcommand functions."""

    def __init__(self, engine: object) -> None:
        """Initialise with the engine object to pass to subcommands."""
        self.obj: dict[str, object] = {"engine": engine}


def _interactive_menu(ctx: typer.Context, engine: object) -> None:
    """Run the top-level interactive menu loop.

    :param ctx: Typer context (unused beyond this function).
    :param engine: SQLAlchemy engine to pass to subcommand functions.
    """
    print("Welcome! Connected to the database.")
    simple_ctx = _SimpleCtx(engine)

    while True:
        choice = ask_choice(
            [
                "Search the database",
                "Add an entry",
                "Update an entry",
                "Delete an entry",
                "(Q)uit",
            ],
            prompt="Your choice: ",
        )
        if choice == 4:  # Quit
            print("Closing connection...")
            break
        elif choice == 0:
            search.search_cmd(simple_ctx)  # type: ignore[arg-type]
        elif choice == 1:
            add.add_cmd(simple_ctx)  # type: ignore[arg-type]
        elif choice == 2:
            update.update_cmd(simple_ctx)  # type: ignore[arg-type]
        elif choice == 3:
            delete.delete_cmd(simple_ctx)  # type: ignore[arg-type]
