"""Typer application: wires subcommands and the no-subcommand top menu.

Running ``pdbsearch`` with a subcommand dispatches directly. Running it with no
subcommand drops into the legacy four-option interactive menu (Search / Add /
Update / Quit) via an ``invoke_without_command`` callback. ``import`` and
``migrate`` are subcommand-only admin operations, deliberately absent from the
four-option menu.
"""

from __future__ import annotations

import typer
from sqlalchemy import Engine

from paper_sorts.cli import add as add_cmd
from paper_sorts.cli import delete as delete_cmd
from paper_sorts.cli import importer as import_cmd
from paper_sorts.cli import migrate as migrate_cmd
from paper_sorts.cli import prompts
from paper_sorts.cli import search as search_cmd
from paper_sorts.cli import update as update_cmd
from paper_sorts.config import ConfigError, load_settings
from paper_sorts.db.session import make_engine
from paper_sorts.logging_config import configure_logging

app = typer.Typer(
    add_completion=False,
    help="Off-line paper-database searcher.",
    no_args_is_help=False,
)


def _build_engine(ctx: typer.Context) -> Engine:
    """Build the database engine from the resolved settings on the context.

    :param ctx: the Typer context carrying global options.
    :returns: a configured engine.
    :raises typer.Exit: if no database URL could be resolved.
    """
    settings = ctx.obj
    if not settings.database_url:
        prompts.info(
            "No database URL configured. Provide --database-url, set "
            "PDBSEARCH_DATABASE_URL, a .env entry, or --config/--key."
        )
        raise typer.Exit(code=1)
    return make_engine(settings.database_url)


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    database_url: str | None = typer.Option(None, "--database-url"),
    log_level: str | None = typer.Option(None, "--log-level"),
    log_file: str | None = typer.Option(None, "--log-file"),
    config: str | None = typer.Option(None, "--config", help="Encrypted INI config file."),
    key: str | None = typer.Option(None, "--key", help="Fernet key file."),
) -> None:
    """Resolve settings, configure logging, and run the top menu if idle.

    :param ctx: the Typer context.
    :param database_url: explicit database URL (highest priority).
    :param log_level: stdlib logging level name.
    :param log_file: optional file-log path.
    :param config: path to a Fernet-encrypted INI config file.
    :param key: path to the Fernet key file.
    """
    try:
        settings = load_settings(
            database_url=database_url,
            log_level=log_level,
            log_file=log_file,
            config_path=config,
            key_path=key,
        )
    except ConfigError as exc:
        prompts.info(str(exc))
        raise typer.Exit(code=1) from exc
    configure_logging(settings.log_level, settings.log_file)
    ctx.obj = settings
    if ctx.invoked_subcommand is None:
        _top_menu(settings)


def _top_menu(settings: object) -> None:
    """Run the legacy four-option interactive menu.

    :param settings: the resolved settings (carrying the database URL).
    """
    database_url = getattr(settings, "database_url", "")
    if not database_url:
        prompts.info(
            "No database URL configured. Provide --database-url, set "
            "PDBSEARCH_DATABASE_URL, a .env entry, or --config/--key."
        )
        raise typer.Exit(code=1)
    engine = make_engine(database_url)
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
            search_cmd.run_search(engine)
        elif choice == 1:
            add_cmd.run_add(engine)
        elif choice == 2:
            update_cmd.run_update(engine)


@app.command("search")
def search(ctx: typer.Context) -> None:
    """Search the database by author or title."""
    search_cmd.run_search(_build_engine(ctx))


@app.command("add")
def add(
    ctx: typer.Context,
    bib_file: str | None = typer.Option(None, "--bib-file", help="Single-entry .bib file."),
) -> None:
    """Add a new paper, inline or from a ``.bib`` file."""
    add_cmd.run_add(_build_engine(ctx), bib_file=bib_file)


@app.command("update")
def update(ctx: typer.Context) -> None:
    """Update a paper's title, contents, bibtex, or an author name."""
    update_cmd.run_update(_build_engine(ctx))


@app.command("delete")
def delete(ctx: typer.Context) -> None:
    """Delete a paper after a confirmation."""
    delete_cmd.run_delete(_build_engine(ctx))


@app.command("import")
def import_(
    ctx: typer.Context,
    tex: str = typer.Option(..., "--tex", help="LaTeX overview file."),
    bib: str = typer.Option(..., "--bib", help="Matching .bib file."),
) -> None:
    """Bulk-import papers from a ``.tex`` + ``.bib`` pair (per-paper commit)."""
    import_cmd.run_import(_build_engine(ctx), tex, bib)


@app.command("migrate")
def migrate(ctx: typer.Context) -> None:
    """Converge a personal database onto the canonical schema (idempotent)."""
    settings = ctx.obj
    migrate_cmd.run_migrate(settings.database_url)


if __name__ == "__main__":
    app()
