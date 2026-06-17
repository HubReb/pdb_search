"""Typer application: wires subcommands and the bare-invocation menu.

Invoked bare, ``pdbsearch`` connects and drops into the four-option interactive
menu (Search / Add / Update / Quit), preserving the legacy top-level dialog.
Invoked with a subcommand it runs that operation. ``migrate`` and ``import`` are
admin/scripted operations and are deliberately absent from the menu.
"""

from __future__ import annotations

from dataclasses import dataclass

import typer
from rich.console import Console
from sqlalchemy import Engine

from paper_sorts.cli import add as add_cmd
from paper_sorts.cli import delete as delete_cmd
from paper_sorts.cli import importer as import_cmd
from paper_sorts.cli import migrate as migrate_cmd
from paper_sorts.cli import search as search_cmd
from paper_sorts.cli import update as update_cmd
from paper_sorts.cli.prompts import ask_text
from paper_sorts.config import ConfigError, load_settings
from paper_sorts.db.session import create_db_engine
from paper_sorts.logging_config import configure_logging

console = Console()


@dataclass
class AppContext:
    """Shared CLI state carried on the Typer context.

    :ivar engine: the SQLAlchemy engine bound to the configured database.
    """

    engine: Engine


app = typer.Typer(
    help="Offline paper-database searcher.",
    no_args_is_help=False,
    invoke_without_command=True,
)


@app.callback()
def main(
    ctx: typer.Context,
    database_url: str | None = typer.Option(None, "--database-url"),
    log_level: str = typer.Option("INFO", "--log-level"),
    log_file: str | None = typer.Option(None, "--log-file"),
    config: str | None = typer.Option(None, "--config"),
    key: str | None = typer.Option(None, "--key"),
) -> None:
    """Resolve config, configure logging, and dispatch to a subcommand or menu.

    :param ctx: the Typer context (carries the :class:`AppContext`).
    :param database_url: explicit database URL (highest-priority source).
    :param log_level: logging level name.
    :param log_file: optional log file path.
    :param config: path to a Fernet-encrypted INI (lowest-priority source).
    :param key: path to the Fernet key file.
    """
    configure_logging(log_level, log_file)
    try:
        settings = load_settings(
            database_url=database_url,
            log_level=log_level,
            log_file=log_file,
            config_file=config,
            key_file=key,
        )
        url = settings.require_database_url()
    except ConfigError as exc:
        console.print(f"[red]Configuration error:[/red] {exc}")
        raise typer.Exit(code=1) from exc

    ctx.obj = AppContext(engine=create_db_engine(url))

    if ctx.invoked_subcommand is None:
        _interactive_menu(ctx.obj)


def _interactive_menu(app_ctx: AppContext) -> None:
    """Run the four-option top-level menu until the user quits.

    :param app_ctx: the shared CLI state.
    """
    console.print("Welcome! Connected to the database.")
    while True:
        choice = ask_text(
            "What do you want to do?\n"
            "1) Search the database\n"
            "2) Add an entry\n"
            "3) Update an entry\n"
            "4) (Q)uit\n"
            "Your choice"
        ).lower()
        match choice:
            case "1":
                search_cmd.run_search(app_ctx.engine)
            case "2":
                add_cmd.run_add(app_ctx.engine)
            case "3":
                update_cmd.run_update(app_ctx.engine)
            case "4" | "q":
                console.print("Closing connection...")
                return
            case _:
                console.print("Your input was invalid")


@app.command()
def search(ctx: typer.Context) -> None:
    """Search the database by author or paper title."""
    app_ctx: AppContext = ctx.obj
    search_cmd.run_search(app_ctx.engine)


@app.command()
def add(ctx: typer.Context) -> None:
    """Add a new entry (inline or from a ``.bib`` file)."""
    app_ctx: AppContext = ctx.obj
    add_cmd.run_add(app_ctx.engine)


@app.command()
def update(ctx: typer.Context) -> None:
    """Update an entry's title, contents, bibtex, or author."""
    app_ctx: AppContext = ctx.obj
    update_cmd.run_update(app_ctx.engine)


@app.command()
def delete(ctx: typer.Context) -> None:
    """Delete an entry and its dependent rows."""
    app_ctx: AppContext = ctx.obj
    delete_cmd.run_delete(app_ctx.engine)


@app.command(name="import")
def import_(
    ctx: typer.Context,
    tex: str = typer.Option(..., "--tex", help="LaTeX literature overview"),
    bib: str = typer.Option(..., "--bib", help="matching .bib file"),
) -> None:
    """Bulk-import papers from a ``.tex`` + ``.bib`` pair (per-paper commit)."""
    app_ctx: AppContext = ctx.obj
    import_cmd.run_import(app_ctx.engine, tex, bib)


@app.command()
def migrate(ctx: typer.Context) -> None:
    """Apply Alembic migrations / converge a legacy schema to canonical."""
    app_ctx: AppContext = ctx.obj
    migrate_cmd.run_migrate(app_ctx.engine)


if __name__ == "__main__":  # pragma: no cover
    app()
