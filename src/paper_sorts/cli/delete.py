"""Delete subcommand for paper_sorts CLI."""

import logging
import sys

import typer
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from paper_sorts.cli import prompts
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)

app = typer.Typer(help="Delete a paper from the database.")


def run_delete(session: Session) -> None:
    """Interactive delete flow: search → pick paper → confirm.

    :param session: Active SQLAlchemy session.
    """
    term = prompts.ask_text("Search term (title)")
    results = paper_service.search_by_title(session, term)
    if not results:
        typer.echo("No papers found.")
        return

    paper_options = [p.title for p in results] + ["Abort"]
    pick = prompts.ask_choice(paper_options, "Select paper to delete")
    if pick == len(paper_options) - 1:
        typer.echo("Delete aborted.")
        return
    paper = results[pick]

    confirmed = prompts.ask_confirmation(f"delete {paper.title!r} (id={paper.id})")
    if not confirmed:
        typer.echo("Delete aborted.")
        return

    paper_service.delete_paper(session, paper.id)
    typer.echo(f"Deleted: {paper.title}")


@app.callback(invoke_without_command=True)
def delete(ctx: typer.Context) -> None:
    """Delete a paper from the database."""
    if ctx.resilient_parsing:
        return

    session: Session | None = ctx.obj.get("session") if ctx.obj else None
    if session is not None:
        try:
            run_delete(session)
        except Exception as exc:
            logger.exception("Delete failed: %s", exc)
            typer.echo(f"Delete failed: {exc}", err=True)
            sys.exit(1)
        return

    engine: Engine | None = ctx.obj.get("engine") if ctx.obj else None
    if engine is None:
        logger.error("No database engine available")
        typer.echo("Error: database not configured.", err=True)
        sys.exit(1)
    try:
        with with_session(engine) as s:
            run_delete(s)
    except LookupError as exc:
        logger.error("Delete failed: %s", exc)
        typer.echo(f"Paper not found: {exc}", err=True)
        sys.exit(1)
    except Exception as exc:
        logger.exception("Delete failed: %s", exc)
        typer.echo(f"Delete failed: {exc}", err=True)
        sys.exit(1)
