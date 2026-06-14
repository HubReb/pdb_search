"""Update subcommand for paper_sorts CLI."""

import logging
import sys

import typer
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from paper_sorts.cli import prompts
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)

app = typer.Typer(help="Update a field on an existing paper.")


def run_update(session: Session) -> None:
    """Interactive update flow: search → pick paper → pick field → confirm.

    :param session: Active SQLAlchemy session.
    """
    term = prompts.ask_text("Search term (title or author)")
    results = paper_service.search_by_title(session, term)
    if not results:
        results = paper_service.search_by_author(session, term)
    if not results:
        typer.echo("No papers found.")
        return

    paper_options = [p.title for p in results] + ["Abort"]
    pick = prompts.ask_choice(paper_options, "Select paper to update")
    if pick == len(paper_options) - 1:
        typer.echo("Update aborted.")
        return
    paper = results[pick]

    field_options = ["title", "contents", "bibtex entry", "author", "Abort"]
    field_pick = prompts.ask_choice(field_options, "Select field to update")
    if field_pick == len(field_options) - 1:
        typer.echo("Update aborted.")
        return

    field_map = ["title", "contents", "bibtex", "author"]
    field = field_map[field_pick]

    new_value = prompts.ask_text(f"New value for {field}")
    confirmed = prompts.ask_confirmation(
        f"update {field!r} of {paper.title!r} to {new_value!r}"
    )
    if not confirmed:
        typer.echo("Update aborted.")
        return

    paper_service.update_field(session, paper.id, field, new_value)  # type: ignore[arg-type]
    typer.echo(f"Updated {field} for: {paper.title}")


@app.callback(invoke_without_command=True)
def update(ctx: typer.Context) -> None:
    """Update a field on an existing paper."""
    if ctx.resilient_parsing:
        return

    session: Session | None = ctx.obj.get("session") if ctx.obj else None
    if session is not None:
        try:
            run_update(session)
        except Exception as exc:
            logger.exception("Update failed: %s", exc)
            typer.echo(f"Update failed: {exc}", err=True)
            sys.exit(1)
        return

    engine: Engine | None = ctx.obj.get("engine") if ctx.obj else None
    if engine is None:
        logger.error("No database engine available")
        typer.echo("Error: database not configured.", err=True)
        sys.exit(1)
    try:
        with with_session(engine) as s:
            run_update(s)
    except LookupError as exc:
        logger.error("Update failed: %s", exc)
        typer.echo(f"Paper not found: {exc}", err=True)
        sys.exit(1)
    except Exception as exc:
        logger.exception("Update failed: %s", exc)
        typer.echo(f"Update failed: {exc}", err=True)
        sys.exit(1)
