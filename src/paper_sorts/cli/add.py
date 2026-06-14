"""Add subcommand for pdbsearch CLI.

Interactively collects paper metadata from the user and inserts a new
paper into the database.
"""

from __future__ import annotations

import logging

from rich.console import Console

from paper_sorts.cli.prompts import ask_choice, ask_file, ask_text
from paper_sorts.db.repositories import PaperCreate
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)
console = Console()


def add_callback(db_url: str) -> None:
    """Interactive add subcommand entrypoint.

    Prompts the user for all required paper fields and inserts the new entry
    into the database. BibTeX information can be entered inline or read from
    a ``.bib`` file.

    Args:
        db_url: SQLAlchemy-compatible database URL from the app callback.
    """
    console.print("[bold]Add new paper[/bold]")

    author_str = ask_text(
        "Author(s) — please provide a comma-separated list (e.g. Smith, J., Doe, A.): "
    )
    paper_title = ask_text("Paper title: ")
    bibtex_key = ask_text("BibTeX key: ")

    bib_options = ["Enter from a file", "Enter inline"]
    bib_idx = ask_choice(
        "How do you want to provide the BibTeX entry?",
        bib_options,
    )

    if bib_idx == 0:
        bib_file = ask_file("Enter the path to the .bib file: ")
        with open(bib_file, encoding="utf-8") as f:
            bibtex_information = f.read()
    else:
        bibtex_information = ask_text("BibTeX entry: ")

    contents = ask_text("Summary of the paper (one sentence): ")
    authors = [a.strip() for a in author_str.split(",") if a.strip()]

    paper = PaperCreate(
        title=paper_title,
        contents=contents,
        bibtex_id=bibtex_key,
        bibtex=bibtex_information,
        authors=authors,
    )

    try:
        paper_service.add_paper(db_url, paper)
        console.print(f"[green]Paper {paper_title!r} added successfully.[/green]")
        logger.info("Added paper %r to database.", paper_title)
    except ValueError as exc:
        console.print(f"[red]Could not add paper — {exc}[/red]")
        logger.error("Failed to add paper %r: %s", paper_title, exc)
