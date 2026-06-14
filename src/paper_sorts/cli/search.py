"""Search subcommand for paper_sorts CLI."""

import logging
import sys

import typer
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from paper_sorts.cli import prompts
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)

app = typer.Typer(help="Search papers by title or author.")


def run_search(session: Session) -> None:
    """Interactive search flow: choose title/author, enter term, display result.

    :param session: Active SQLAlchemy session.
    """
    mode_options = ["Search by title", "Search by author", "Back / abort"]
    choice = prompts.ask_choice(mode_options, "Search by")

    if choice == len(mode_options) - 1:
        typer.echo("Search aborted.")
        return

    if choice == 0:
        term = prompts.ask_text("Enter title (or part of title)")
        results = paper_service.search_by_title(session, term)
    else:
        term = prompts.ask_text("Enter author name (or part of name)")
        results = paper_service.search_by_author(session, term)

    if not results:
        typer.echo("No papers found.")
        return

    if len(results) == 1:
        prompts.pretty_print_paper(results[0])
        return

    # Multiple results: ask user to pick one
    paper_options = [p.title for p in results] + ["Abort"]
    pick = prompts.ask_choice(paper_options, "Select paper")
    if pick == len(paper_options) - 1:
        typer.echo("Search aborted.")
        return
    prompts.pretty_print_paper(results[pick])


@app.callback(invoke_without_command=True)
def search(ctx: typer.Context) -> None:
    """Search papers by title or author interactively."""
    if ctx.resilient_parsing:
        return
    # Prefer injected session (interactive mode), fall back to creating from engine
    session: Session | None = ctx.obj.get("session") if ctx.obj else None
    if session is not None:
        try:
            run_search(session)
        except Exception as exc:
            logger.exception("Search failed: %s", exc)
            typer.echo(f"Search failed: {exc}", err=True)
            sys.exit(1)
        return

    engine: Engine | None = ctx.obj.get("engine") if ctx.obj else None
    if engine is None:
        logger.error("No database engine available")
        typer.echo("Error: database not configured.", err=True)
        sys.exit(1)
    try:
        with with_session(engine) as s:
            run_search(s)
    except Exception as exc:
        logger.exception("Search failed: %s", exc)
        typer.echo(f"Search failed: {exc}", err=True)
        sys.exit(1)
