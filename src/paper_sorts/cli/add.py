"""Add subcommand for paper_sorts CLI.

Prompts for paper metadata and persists the entry.  All prompts route through
cli/prompts.py (constitution Principle III).
"""

from __future__ import annotations

import logging
from pathlib import Path

import typer
from rich.console import Console

from paper_sorts.cli.prompts import ask_str
from paper_sorts.db.repositories import PaperCreate
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)
app = typer.Typer(help="Add a new paper to the database.")
_console = Console()


@app.callback(invoke_without_command=True)
def add_callback(ctx: typer.Context) -> None:
    """Run add flow when invoked as subcommand.

    Args:
        ctx: Typer context.
    """
    if ctx.invoked_subcommand is None:
        from paper_sorts.cli.app import get_database_url

        run_add(get_database_url())


def run_add(database_url: str) -> None:
    """Interactive add paper flow.

    Prompts for all required fields, re-prompting on empty input.
    Accepts either a raw BibTeX string or a path to a .bib file.

    Args:
        database_url: SQLAlchemy connection string.
    """
    title = ask_str("Enter title")
    authors_raw = ask_str("Enter authors (comma-separated, Last First format)")
    bibtex_id = ask_str("Enter BibTeX key")
    contents = ask_str("Enter summary")

    bib_entry = ask_str("Enter BibTeX entry or path to .bib file")
    # If it looks like a file path, try reading it
    if bib_entry.endswith(".bib") or Path(bib_entry).exists():
        try:
            bib_entry = Path(bib_entry).read_text(encoding="utf-8")
        except OSError as exc:
            _console.print(f"[red]Could not read .bib file: {exc}[/red]")
            return

    authors = [a.strip() for a in authors_raw.split(",") if a.strip()]
    paper = PaperCreate(
        title=title,
        contents=contents,
        bibtex_id=bibtex_id,
        bibtex=bib_entry,
        authors=authors,
    )

    try:
        with with_session(database_url) as session:
            result = paper_service.add_paper(session, paper)
        _console.print(f"[green]Paper added successfully.[/green] (id={result.id})")
    except Exception as exc:
        logger.warning("add_paper failed: %s", exc)
        _console.print(f"[red]Could not add paper: {exc}[/red]")
