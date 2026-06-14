"""Add subcommand for paper_sorts CLI."""

import logging
import sys
from pathlib import Path

import typer
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from paper_sorts.cli import prompts
from paper_sorts.db.repositories import PaperCreate
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)

app = typer.Typer(help="Add a new paper entry.")


def _add_from_bib_file(session: Session, bib_path: str) -> None:
    """Add a single paper by reading a .bib file.

    :param session: Active SQLAlchemy session.
    :param bib_path: Path to a .bib file with exactly one entry.
    """
    from pybtex.database import parse_file as parse_bib

    try:
        bib_graph = parse_bib(bib_path, bib_format="bibtex")
    except Exception as exc:
        logger.error("Failed to parse .bib file: %s", exc)
        typer.echo(f"Could not read .bib file: {exc}", err=True)
        return

    keys = list(bib_graph.entries.keys())
    if not keys:
        typer.echo("No entries found in the .bib file.", err=True)
        return

    bib_key = keys[0]
    entry = bib_graph.entries[bib_key]
    bibtex_str = entry.to_string("bibtex")

    authors: list[str] = []
    for person in entry.persons.get("author", []):
        last = person.last_names[0] if person.last_names else ""
        first = person.first_names[0] if person.first_names else ""
        if last or first:
            authors.append(f"{last}, {first}" if first else last)

    title_field = entry.fields.get("title", "")
    title = title_field if title_field else prompts.ask_text("Title")
    contents = prompts.ask_text("Summary / abstract")

    data = PaperCreate(
        title=title,
        contents=contents,
        bibtex_id=bib_key,
        bibtex=bibtex_str,
        authors=authors,
    )
    paper = paper_service.add_paper(session, data)
    typer.echo(f"Added: {paper.title} (id={paper.id})")


def _add_manually(session: Session) -> None:
    """Prompt for all fields and add a paper.

    :param session: Active SQLAlchemy session.
    """
    title = prompts.ask_text("Title")
    author_str = prompts.ask_text("Author(s) — semicolon-separated, 'Last, First' format")
    authors = [a.strip() for a in author_str.split(";") if a.strip()]
    bibtex_id = prompts.ask_text("BibTeX key")
    contents = prompts.ask_text("Summary / abstract")
    bibtex = prompts.ask_text("BibTeX entry (paste full entry)")

    data = PaperCreate(
        title=title,
        contents=contents,
        bibtex_id=bibtex_id,
        bibtex=bibtex,
        authors=authors,
    )
    paper = paper_service.add_paper(session, data)
    typer.echo(f"Added: {paper.title} (id={paper.id})")


def run_add(session: Session) -> None:
    """Interactive add flow.

    :param session: Active SQLAlchemy session.
    """
    bib_path = prompts.ask_bibtex_file()
    if bib_path:
        if not Path(bib_path).exists():
            typer.echo(f"File not found: {bib_path}", err=True)
            return
        _add_from_bib_file(session, bib_path)
    else:
        _add_manually(session)


@app.callback(invoke_without_command=True)
def add(ctx: typer.Context) -> None:
    """Add a new paper to the database."""
    if ctx.resilient_parsing:
        return

    session: Session | None = ctx.obj.get("session") if ctx.obj else None
    if session is not None:
        try:
            run_add(session)
        except Exception as exc:
            logger.exception("Add failed: %s", exc)
            typer.echo(f"Add failed: {exc}", err=True)
            sys.exit(1)
        return

    engine: Engine | None = ctx.obj.get("engine") if ctx.obj else None
    if engine is None:
        logger.error("No database engine available")
        typer.echo("Error: database not configured.", err=True)
        sys.exit(1)
    try:
        with with_session(engine) as s:
            run_add(s)
    except ValueError as exc:
        logger.error("Add failed: %s", exc)
        typer.echo(f"Could not add paper: {exc}", err=True)
        sys.exit(1)
    except Exception as exc:
        logger.exception("Add failed: %s", exc)
        typer.echo(f"Add failed: {exc}", err=True)
        sys.exit(1)
