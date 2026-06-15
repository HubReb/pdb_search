"""The Typer application: subcommands plus the four-option top-level menu.

``pdbsearch`` with no subcommand drops into the legacy interactive menu. The
``import`` and ``migrate`` subcommands are admin/scripted operations and are
deliberately absent from that menu. Logging and settings are configured once at
startup from the four-source :class:`~paper_sorts.config.Settings` chain.
"""

from __future__ import annotations

import typer
from sqlalchemy import Engine

from paper_sorts.cli import prompts
from paper_sorts.cli.add import run_add
from paper_sorts.cli.delete import run_delete
from paper_sorts.cli.importer import run_import
from paper_sorts.cli.migrate import run_migrate
from paper_sorts.cli.search import run_search
from paper_sorts.cli.update import run_update
from paper_sorts.config import ConfigurationError, Settings, load_settings
from paper_sorts.db.session import create_db_engine
from paper_sorts.logging_config import configure_logging
from paper_sorts.services.paper_service import PaperService

app = typer.Typer(
    help="Off-line paper-database searcher.",
    no_args_is_help=False,
    invoke_without_command=True,
)


def _settings(ctx: typer.Context) -> Settings:
    """Return the resolved settings stored on the Typer context."""
    settings: Settings = ctx.obj
    return settings


def _engine(ctx: typer.Context) -> Engine:
    """Build a database engine from the context settings.

    :raises typer.Exit: with a plain-language message if no database URL resolves.
    """
    settings = _settings(ctx)
    if not settings.database_url:
        prompts.error(
            "No database configured. Set PDBSEARCH_DATABASE_URL, pass "
            "--database-url, or supply --config/--key."
        )
        raise typer.Exit(code=1)
    return create_db_engine(settings.database_url)


@app.callback()
def main(
    ctx: typer.Context,
    database_url: str | None = typer.Option(None, "--database-url", help="SQLAlchemy URL"),
    config: str | None = typer.Option(None, "--config", help="Fernet-encrypted INI file"),
    key: str | None = typer.Option(None, "--key", help="Fernet key file"),
    log_level: str | None = typer.Option(None, "--log-level", help="DEBUG/INFO/WARNING/ERROR"),
) -> None:
    """Resolve settings, configure logging, and run the menu if no subcommand.

    :param ctx: the Typer context (carries resolved settings to subcommands).
    :param database_url: optional explicit database URL (highest priority).
    :param config: optional Fernet-encrypted INI config path.
    :param key: optional Fernet key path.
    :param log_level: optional logging level override.
    """
    try:
        settings = load_settings(
            database_url=database_url,
            config=config,
            key=key,
            log_level=log_level,
        )
    except ConfigurationError as exc:
        prompts.error(str(exc))
        raise typer.Exit(code=1) from exc
    configure_logging(settings.log_level, settings.log_file)
    ctx.obj = settings
    if ctx.invoked_subcommand is None:
        _run_menu(settings)


def _run_menu(settings: Settings) -> None:
    """Run the four-option top-level interactive menu.

    :param settings: the resolved settings (must carry a database URL).
    """
    if not settings.database_url:
        prompts.error(
            "No database configured. Set PDBSEARCH_DATABASE_URL, pass "
            "--database-url, or supply --config/--key."
        )
        raise typer.Exit(code=1)
    service = PaperService(create_db_engine(settings.database_url))
    while True:
        choice = prompts.ask_choice(
            "What do you want to do?",
            ["Search the database", "Add an entry", "Update an entry"],
            abort_label="(Q)uit",
        )
        if choice is None:
            prompts.info("Closing connection...")
            return
        if choice == 0:
            run_search(service)
        elif choice == 1:
            run_add(service)
        elif choice == 2:
            run_update(service)


@app.command()
def search(ctx: typer.Context) -> None:
    """Search the database by author or title."""
    run_search(PaperService(_engine(ctx)))


@app.command()
def add(ctx: typer.Context) -> None:
    """Add a new paper (inline or from a .bib file)."""
    run_add(PaperService(_engine(ctx)))


@app.command()
def update(ctx: typer.Context) -> None:
    """Update an existing entry's title, contents, bibtex, or author."""
    run_update(PaperService(_engine(ctx)))


@app.command()
def delete(ctx: typer.Context) -> None:
    """Delete an entry and its dependent rows."""
    run_delete(PaperService(_engine(ctx)))


@app.command("import")
def import_(
    ctx: typer.Context,
    tex: str = typer.Option(..., "--tex", help="LaTeX literature-overview file"),
    bib: str = typer.Option(..., "--bib", help="BibTeX file"),
) -> None:
    """Bulk-import papers from a .tex + .bib pair (admin/scripted)."""
    run_import(_engine(ctx), tex, bib)


@app.command()
def migrate(ctx: typer.Context) -> None:
    """Upgrade the database to the canonical schema (admin/scripted)."""
    settings = _settings(ctx)
    if not settings.database_url:
        prompts.error("No database configured for migration.")
        raise typer.Exit(code=1)
    run_migrate(settings.database_url)


if __name__ == "__main__":
    app()
