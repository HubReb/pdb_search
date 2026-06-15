"""Typer application entry point.

Wires the subcommands (search / add / update / delete / migrate / import) and, when invoked with
no subcommand, drops into the legacy four-option interactive menu. Configuration is resolved
through :func:`paper_sorts.config.load_settings`; logging is configured once at startup. Failure
paths log full detail and surface a short, plain-language message — no stack traces reach
stdout.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated

import typer

from paper_sorts.cli import prompts
from paper_sorts.cli.add import run_add
from paper_sorts.cli.delete import run_delete
from paper_sorts.cli.importer import run_import
from paper_sorts.cli.migrate import run_migrate
from paper_sorts.cli.search import run_search
from paper_sorts.cli.update import run_update
from paper_sorts.config import Settings, load_settings
from paper_sorts.db.session import create_db_engine
from paper_sorts.logging_config import setup_logging
from paper_sorts.services.paper_service import PaperService

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Off-line paper-database searcher.", no_args_is_help=False, add_completion=False
)


def _settings_from_ctx(ctx: typer.Context) -> Settings:
    """Return the resolved :class:`Settings` stored on the Typer context.

    :param ctx: the Typer context.
    :return: the resolved settings.
    """
    obj = ctx.obj
    assert isinstance(obj, Settings)
    return obj


def _service(ctx: typer.Context) -> PaperService:
    """Build a :class:`PaperService` from the context settings.

    :param ctx: the Typer context.
    :return: a paper service bound to the configured database.
    """
    settings = _settings_from_ctx(ctx)
    return PaperService(create_db_engine(settings.database_url))


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    database_url: Annotated[str | None, typer.Option(help="SQLAlchemy database URL.")] = None,
    log_level: Annotated[str | None, typer.Option(help="Logging level.")] = None,
    config: Annotated[Path | None, typer.Option(help="Fernet-encrypted INI config file.")] = None,
    key: Annotated[Path | None, typer.Option(help="Fernet key file.")] = None,
) -> None:
    """Resolve configuration and, with no subcommand, run the interactive menu.

    :param ctx: the Typer context.
    :param database_url: explicit database URL (highest priority).
    :param log_level: explicit log level.
    :param config: path to the Fernet-encrypted INI file.
    :param key: path to the Fernet key file.
    """
    try:
        settings = load_settings(
            database_url=database_url,
            log_level=log_level,
            config_path=config,
            key_path=key,
        )
    except (FileNotFoundError, ValueError) as exc:
        setup_logging("INFO")
        logger.error("configuration error: %s", exc)
        prompts.show("Could not load configuration - please check the logs.")
        raise typer.Exit(code=1) from exc

    setup_logging(settings.log_level)
    ctx.obj = settings

    if ctx.invoked_subcommand is None:
        _interactive_menu(ctx)


def _interactive_menu(ctx: typer.Context) -> None:
    """Run the legacy four-option top-level menu loop.

    :param ctx: the Typer context carrying the resolved settings.
    """
    service = _service(ctx)
    while True:
        choice = prompts.ask_choice(
            "What do you want to do?",
            ["Search the database", "Add an entry", "Update an entry", "(Q)uit"],
        )
        try:
            if choice == 0:
                run_search(service)
            elif choice == 1:
                run_add(service)
            elif choice == 2:
                run_update(service)
            else:
                prompts.show("Closing connection...")
                return
        except Exception as exc:  # noqa: BLE001 - surface plain message, log detail
            logger.exception("operation failed: %s", exc)
            prompts.show("Something went wrong - please check the logs.")


@app.command()
def search(ctx: typer.Context) -> None:
    """Search the database by author or paper title."""
    run_search(_service(ctx))


@app.command()
def add(ctx: typer.Context) -> None:
    """Add a new entry (inline or from a .bib file)."""
    run_add(_service(ctx))


@app.command()
def update(ctx: typer.Context) -> None:
    """Update an existing entry's title, contents, bibtex, or author."""
    run_update(_service(ctx))


@app.command()
def delete(ctx: typer.Context) -> None:
    """Delete an entry."""
    run_delete(_service(ctx))


@app.command()
def migrate(ctx: typer.Context) -> None:
    """Upgrade a personal database to the canonical schema (idempotent)."""
    settings = _settings_from_ctx(ctx)
    run_migrate(settings.database_url)


@app.command(name="import")
def import_cmd(
    ctx: typer.Context,
    tex: Annotated[Path, typer.Option(help="LaTeX literature-overview file.")],
    bib: Annotated[Path, typer.Option(help="BibTeX file with matching entries.")],
) -> None:
    """Bulk-import every cited entry that has a matching .bib record."""
    run_import(_service(ctx), tex, bib)


if __name__ == "__main__":
    app()
