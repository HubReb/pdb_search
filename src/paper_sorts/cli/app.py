"""Typer application entry point.

Wires the subcommands, resolves configuration through the four-source chain, sets
up logging once, and — when invoked with no subcommand — drops into the legacy
four-option interactive menu (Search / Add / Update / Quit). ``migrate`` and
``import`` are admin/scripted subcommands and are deliberately absent from that
menu.

Failures surface as plain-language messages; technical detail goes to the logger.
No raw exception or stack trace reaches stdout.
"""

from __future__ import annotations

import logging

import typer
from rich.console import Console

from paper_sorts.cli import add as add_cmd
from paper_sorts.cli import delete as delete_cmd
from paper_sorts.cli import importer as import_cmd
from paper_sorts.cli import migrate as migrate_cmd
from paper_sorts.cli import search as search_cmd
from paper_sorts.cli import update as update_cmd
from paper_sorts.cli.prompts import ABORT, ask_choice
from paper_sorts.config import ConfigError, Settings
from paper_sorts.db.session import DbEngine, create_db_engine
from paper_sorts.logging_config import setup_logging

_logger = logging.getLogger(__name__)
_console = Console()

app = typer.Typer(
    help="Offline CLI to store and search publication metadata.",
    no_args_is_help=False,
    add_completion=False,
)


def _resolve_engine(ctx: typer.Context) -> DbEngine:
    """Build the database engine from the resolved settings on the context.

    :param ctx: the Typer context carrying the resolved :class:`Settings`.
    :return: a database engine.
    :raises typer.Exit: with a plain message if no database URL is configured.
    """
    settings: Settings = ctx.obj
    try:
        url = settings.require_database_url()
    except ConfigError as exc:
        _logger.error("configuration error: %s", exc)
        _console.print(str(exc))
        raise typer.Exit(code=1) from exc
    return create_db_engine(url)


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    database_url: str | None = typer.Option(
        None, "--database-url", help="SQLAlchemy URL, e.g. postgresql+psycopg://..."
    ),
    log_level: str | None = typer.Option(None, "--log-level", help="Logging level."),
    config: str | None = typer.Option(None, "--config", help="Fernet-encrypted INI config file."),
    key: str | None = typer.Option(None, "--key", help="Fernet key file for --config."),
) -> None:
    """Resolve configuration, set up logging, and run the menu if no subcommand.

    :param ctx: the Typer context.
    :param database_url: highest-priority database URL override.
    :param log_level: logging level override.
    :param config: encrypted INI config path.
    :param key: Fernet key path for ``config``.
    """
    overrides: dict[str, str] = {}
    if database_url is not None:
        overrides["database_url"] = database_url
    if log_level is not None:
        overrides["log_level"] = log_level
    if config is not None:
        overrides["config_path"] = config
    if key is not None:
        overrides["key_path"] = key

    try:
        settings = Settings(**overrides)  # type: ignore[arg-type]
    except ConfigError as exc:
        # Encrypted-source resolution can fail (e.g. lost key) during construction.
        _console.print(str(exc))
        raise typer.Exit(code=1) from exc

    setup_logging(settings.log_level, settings.log_file)
    ctx.obj = settings

    if ctx.invoked_subcommand is not None:
        return
    _run_menu(ctx)


def _run_menu(ctx: typer.Context) -> None:
    """Run the legacy four-option top-level interactive menu.

    :param ctx: the Typer context (engine resolved lazily per action).
    """
    while True:
        choice = ask_choice(
            "What do you want to do?",
            ["Search the database", "Add an entry", "Update an entry"],
            abort_label="(Q)uit",
        )
        if choice == ABORT:
            return
        engine = _resolve_engine(ctx)
        if choice == 0:
            search_cmd.run_search(engine)
        elif choice == 1:
            add_cmd.run_add(engine)
        elif choice == 2:
            update_cmd.run_update(engine)


@app.command()
def search(ctx: typer.Context) -> None:
    """Search the database by author or paper title."""
    search_cmd.run_search(_resolve_engine(ctx))


@app.command()
def add(ctx: typer.Context) -> None:
    """Add a new paper (inline or from a .bib file)."""
    add_cmd.run_add(_resolve_engine(ctx))


@app.command()
def update(ctx: typer.Context) -> None:
    """Update a paper's title/contents, a BibTeX entry, or an author."""
    update_cmd.run_update(_resolve_engine(ctx))


@app.command()
def delete(ctx: typer.Context) -> None:
    """Delete a paper and its orphaned authors."""
    delete_cmd.run_delete(_resolve_engine(ctx))


@app.command(name="import")
def import_(
    ctx: typer.Context,
    tex: str = typer.Option(..., "--tex", help="Path to the .tex literature file."),
    bib: str = typer.Option(..., "--bib", help="Path to the .bib references file."),
) -> None:
    """Bulk-import every cited entry from a .tex + .bib pair (per-paper commit)."""
    import_cmd.run_import(_resolve_engine(ctx), tex, bib)


@app.command()
def migrate(ctx: typer.Context) -> None:
    """Upgrade a personal database to the canonical schema (idempotent)."""
    settings: Settings = ctx.obj
    try:
        url = settings.require_database_url()
    except ConfigError as exc:
        _console.print(str(exc))
        raise typer.Exit(code=1) from exc
    migrate_cmd.run_migrate(url)


if __name__ == "__main__":  # pragma: no cover
    app()
