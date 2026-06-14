"""Add subcommand for paper_sorts CLI.

Implements ``pdbsearch add``: prompts for author(s), title, bibtex key,
bibtex entry (inline or from file), and summary, then persists the paper.
"""

from __future__ import annotations

import logging
from pathlib import Path

import typer
from rich.console import Console
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

from paper_sorts.cli.prompts import ask_choice, ask_nonempty
from paper_sorts.db.repositories import PaperCreate
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)
console = Console()

app = typer.Typer(help="Add a new paper to the database.")


def run_add(engine: Engine) -> None:
    """Interactively add a new paper to the database.

    Prompts for author(s), title, BibTeX key, BibTeX entry (inline or from
    file), and a summary. Persists the entry and reports success or failure.

    :param engine: Active SQLAlchemy engine.
    """
    authors_raw = ask_nonempty("Author(s) — comma-separated list (Last, First)")
    authors = [a.strip() for a in authors_raw.split(",") if a.strip()]

    title = ask_nonempty("Paper title")
    bibtex_key = ask_nonempty("BibTeX key")

    bibtex_source_choice = ask_choice(
        ["Enter bibtex from a file", "Enter bibtex inline", "(A)bort"],
        "BibTeX entry source",
    )
    if bibtex_source_choice == 3:
        console.print("Add aborted.")
        return

    if bibtex_source_choice == 1:
        bib_file = ask_nonempty("Path to .bib file")
        bib_path = Path(bib_file)
        if not bib_path.exists():
            console.print(f"[red]File not found: {bib_path}[/red]")
            logger.error("Bib file not found: %s", bib_path)
            return
        bibtex_str = bib_path.read_text(encoding="utf-8")
    else:
        bibtex_str = ask_nonempty("BibTeX entry (paste full entry)")

    contents = ask_nonempty("Summary / abstract")

    paper = PaperCreate(
        title=title,
        contents=contents,
        bibtex_id=bibtex_key,
        bibtex=bibtex_str,
        authors=authors,
    )

    try:
        result = paper_service.add_paper(engine, paper)
        console.print(f"[green]Added paper (id={result.id}): {result.title}[/green]")
        logger.info("Added paper id=%d title=%r", result.id, result.title)
    except IntegrityError as exc:
        console.print(
            "[red]Could not add paper — a paper with the same BibTeX key or entry "
            "already exists. Check logs for details.[/red]"
        )
        logger.error("IntegrityError adding paper: %s", exc)
    except Exception as exc:
        console.print(
            "[red]An unexpected error occurred while adding the paper. "
            "Check logs for details.[/red]"
        )
        logger.exception("Unexpected error adding paper: %s", exc)


@app.callback(invoke_without_command=True)
def add_cmd(ctx: typer.Context) -> None:
    """Interactively add a new paper to the database.

    Prompts for author(s), title, BibTeX key, BibTeX entry (inline or from
    file), and a summary. Persists the entry and reports success or failure.
    """
    if ctx.invoked_subcommand is not None:
        return
    engine: Engine = ctx.obj["engine"]
    run_add(engine)
