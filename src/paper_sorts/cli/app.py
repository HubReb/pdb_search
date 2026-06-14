"""Main Typer application for paper_sorts CLI.

Entry point: pdbsearch (via [project.scripts] in pyproject.toml).

When invoked with no subcommand, drops into an interactive four-option
top-level menu (search / add / update / delete / quit).

'migrate' and 'import' subcommands are not in the top-level menu —
they are admin/scripted operations.
"""

from __future__ import annotations

import logging
import sys

import typer
from sqlalchemy.orm import Session

from paper_sorts.cli import add as add_cmd
from paper_sorts.cli import delete as delete_cmd
from paper_sorts.cli import importer as importer_cmd
from paper_sorts.cli import migrate as migrate_cmd
from paper_sorts.cli import prompts
from paper_sorts.cli import search as search_cmd
from paper_sorts.cli import update as update_cmd
from paper_sorts.config import Settings
from paper_sorts.db.session import create_engine_from_url, with_session
from paper_sorts.logging_config import configure_logging

logger = logging.getLogger(__name__)

app = typer.Typer(
    name="pdbsearch",
    help="Off-line paper-database searcher. Invoke without a subcommand for interactive mode.",
    invoke_without_command=True,
)

# Register subcommands
app.add_typer(search_cmd.app, name="search")
app.add_typer(add_cmd.app, name="add")
app.add_typer(update_cmd.app, name="update")
app.add_typer(delete_cmd.app, name="delete")
app.add_typer(importer_cmd.app, name="import")
app.add_typer(migrate_cmd.app, name="migrate")


@app.callback()
def root_callback(
    ctx: typer.Context,
    database_url: str | None = typer.Option(
        None, "--database-url", envvar="PDBSEARCH_DATABASE_URL", help="PostgreSQL DSN"
    ),
    log_level: str = typer.Option(
        "INFO", "--log-level", envvar="PDBSEARCH_LOG_LEVEL", help="Logging level"
    ),
    config: str | None = typer.Option(
        None, "--config", envvar="PDBSEARCH_CONFIG", help="Path to encrypted INI config"
    ),
    key: str | None = typer.Option(
        None, "--key", envvar="PDBSEARCH_KEY", help="Path to Fernet key file"
    ),
) -> None:
    """Set up logging, resolve settings, and inject session into context.

    If no subcommand is given, drops into the interactive top-level menu.
    """
    configure_logging(log_level)

    ctx.ensure_object(dict)

    from pathlib import Path

    settings = Settings(
        database_url=database_url or "",
        log_level=log_level,
        fernet_config_path=Path(config) if config else None,
        fernet_key_path=Path(key) if key else None,
    )
    resolved_url = database_url or settings.database_url

    # Store URL and engine lazily — subcommands get it from ctx.obj.
    # Missing URL is only an error when we actually try to use the DB.
    ctx.obj["database_url"] = resolved_url

    if resolved_url:
        engine = create_engine_from_url(resolved_url)
        ctx.obj["engine"] = engine
    else:
        ctx.obj["engine"] = None

    if ctx.invoked_subcommand is None:
        # Interactive top-level menu — DB is required here
        if not resolved_url:
            typer.echo(
                "Error: no database URL. Use --database-url or PDBSEARCH_DATABASE_URL.",
                err=True,
            )
            sys.exit(1)
        engine = ctx.obj["engine"]
        with with_session(engine) as session:
            ctx.obj["session"] = session
            _run_interactive_menu(session)


def _run_interactive_menu(session: Session) -> None:
    """Run the four-option interactive top-level menu.

    :param session: Active SQLAlchemy session for the interactive loop.
    """
    typer.echo("Welcome to paper_sorts!")
    menu_options = ["Search papers", "Add a paper", "Update a paper", "Delete a paper", "Quit"]
    while True:
        pick = prompts.ask_choice(menu_options, "Choose an action")
        if pick == 0:
            search_cmd.run_search(session)
            session.commit()
        elif pick == 1:
            add_cmd.run_add(session)
            session.commit()
        elif pick == 2:
            update_cmd.run_update(session)
            session.commit()
        elif pick == 3:
            delete_cmd.run_delete(session)
            session.commit()
        elif pick == len(menu_options) - 1:
            typer.echo("Goodbye!")
            break


def main() -> None:
    """Entry point for the pdbsearch CLI script."""
    app()
