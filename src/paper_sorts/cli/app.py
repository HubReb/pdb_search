"""Typer application: subcommands plus the no-subcommand top-level menu.

The root callback resolves settings (four-source chain), configures logging, and
builds a :class:`PaperService` bound to the configured engine. Invoked with no
subcommand, it drops into the legacy four-option menu (Search / Add / Update /
Quit). ``import`` and ``migrate`` are admin/scripted operations, deliberately
absent from that menu.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import typer

from paper_sorts.cli import add as add_cli
from paper_sorts.cli import delete as delete_cli
from paper_sorts.cli import importer as import_cli
from paper_sorts.cli import migrate as migrate_cli
from paper_sorts.cli import prompts
from paper_sorts.cli import search as search_cli
from paper_sorts.cli import update as update_cli
from paper_sorts.config import ConfigError, load_settings
from paper_sorts.db.session import make_engine
from paper_sorts.logging_config import configure_logging
from paper_sorts.services.paper_service import PaperService

_logger = logging.getLogger(__name__)

app = typer.Typer(help="Offline personal paper-database searcher.", no_args_is_help=False)


@dataclass
class AppContext:
    """Per-invocation context shared with subcommands."""

    service: PaperService
    database_url: str


def _build_context(
    database_url: str | None,
    log_level: str | None,
    config: Path | None,
    key: Path | None,
) -> AppContext:
    settings = load_settings(
        database_url=database_url,
        log_level=log_level,
        config_path=config,
        key_path=key,
    )
    configure_logging(settings.log_level, settings.log_file)
    if not settings.database_url:
        raise typer.BadParameter(
            "No database configured. Pass --database-url, set PDBSEARCH_DATABASE_URL, "
            "or provide --config/--key."
        )
    engine = make_engine(settings.database_url)
    return AppContext(service=PaperService(engine), database_url=settings.database_url)


def _top_level_menu(service: PaperService) -> None:
    """Run the legacy four-option interactive menu."""
    while True:
        choice = prompts.ask_choice(
            "What do you want to do?",
            ["Search the database", "Add an entry", "Update an entry"],
        )
        if choice is None:
            prompts.info("Closing connection...")
            return
        if choice == 0:
            search_cli.run_search(service)
        elif choice == 1:
            add_cli.run_add(service)
        elif choice == 2:
            update_cli.run_update(service)


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    database_url: str | None = typer.Option(None, "--database-url", help="SQLAlchemy URL"),
    log_level: str | None = typer.Option(None, "--log-level", help="DEBUG/INFO/WARNING/ERROR"),
    config: Path | None = typer.Option(None, "--config", help="Fernet-encrypted INI path"),
    key: Path | None = typer.Option(None, "--key", help="Fernet key path"),
) -> None:
    """Resolve config, configure logging, and dispatch."""
    try:
        context = _build_context(database_url, log_level, config, key)
    except ConfigError as exc:
        prompts.info(str(exc))
        raise typer.Exit(code=1) from exc
    ctx.obj = context
    if ctx.invoked_subcommand is None:
        _top_level_menu(context.service)


@app.command()
def search(ctx: typer.Context) -> None:
    """Interactively search by author or title."""
    search_cli.run_search(_ctx(ctx).service)


@app.command()
def add(ctx: typer.Context) -> None:
    """Interactively add a new paper."""
    add_cli.run_add(_ctx(ctx).service)


@app.command()
def update(ctx: typer.Context) -> None:
    """Interactively update an existing entry."""
    update_cli.run_update(_ctx(ctx).service)


@app.command()
def delete(ctx: typer.Context) -> None:
    """Interactively delete an entry."""
    delete_cli.run_delete(_ctx(ctx).service)


@app.command(name="import")
def import_(
    ctx: typer.Context,
    tex: Path = typer.Option(..., "--tex", help="LaTeX literature overview"),
    bib: Path = typer.Option(..., "--bib", help="BibTeX file"),
) -> None:
    """Bulk-import papers from a .tex + .bib pair (per-paper commit)."""
    import_cli.run_import(_ctx(ctx).service, tex, bib)


@app.command()
def migrate(ctx: typer.Context) -> None:
    """Upgrade the configured database to the modern schema (idempotent)."""
    migrate_cli.run_migrate(_ctx(ctx).database_url)


def _ctx(ctx: typer.Context) -> AppContext:
    obj = ctx.obj
    assert isinstance(obj, AppContext)
    return obj


if __name__ == "__main__":
    app()
