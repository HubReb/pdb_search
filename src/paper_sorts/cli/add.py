"""Add subcommand for paper_sorts CLI."""

from __future__ import annotations

import logging
from pathlib import Path

import typer

from paper_sorts.cli.prompts import ask_choice, ask_confirm, ask_str
from paper_sorts.db.repositories import PaperCreate
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service

log = logging.getLogger(__name__)

app = typer.Typer(help="Add a new paper to the database.")


def _run_add(database_url: str) -> None:
    """Execute the interactive add flow.

    :param database_url: SQLAlchemy-compatible database URL.
    """
    author_raw = ask_str("Author(s) — comma-separated (e.g. Smith, J., Doe, A.)")
    authors = [a.strip() for a in author_raw.split(",") if a.strip()]
    # Re-pair "Last, First" entries that were split by the outer comma
    # Heuristic: if element contains no space it is likely a first-name fragment.
    # Better: ask users to use semicolons. For now, keep legacy comma-sep behaviour.
    # Legacy stored "Last, First" joined with ", " — split on ", " pairs.
    paired: list[str] = []
    i = 0
    while i < len(authors):
        # If next element has no space and looks like a first name, pair it
        if i + 1 < len(authors) and " " not in authors[i + 1] and len(authors[i + 1]) <= 3:
            paired.append(f"{authors[i]}, {authors[i + 1]}")
            i += 2
        else:
            paired.append(authors[i])
            i += 1
    if not paired:
        paired = authors

    title = ask_str("Paper title")
    bibtex_key = ask_str("BibTeX key")

    bib_choice = ask_choice(
        "BibTeX entry — provide inline or from file",
        ["Enter inline", "Load from file"],
    )
    if bib_choice is None:
        typer.echo("Add aborted.")
        return
    if bib_choice == 2:
        bib_file = ask_str("BibTeX filename")
        try:
            bibtex = Path(bib_file).read_text(encoding="utf-8")
        except OSError as exc:
            typer.echo(f"Could not read file: {exc}")
            log.error("Could not read BibTeX file '%s': %s", bib_file, exc)
            return
    else:
        bibtex = ask_str("BibTeX entry")

    contents = ask_str("Summary of the paper (one sentence)")

    author_display = " and ".join(paired)
    confirmed = ask_confirm(
        f"Add paper '{title}' by {author_display}?"
    )
    if not confirmed:
        typer.echo("Add aborted.")
        return

    paper = PaperCreate(
        title=title,
        contents=contents,
        bibtex_id=bibtex_key,
        bibtex=bibtex,
        authors=paired,
    )
    try:
        with with_session(database_url) as session:
            paper_service.add_paper(session, paper)
        typer.echo(f"Paper '{title}' added successfully.")
        log.info("Added paper '%s' (%s).", title, bibtex_key)
    except ValueError as exc:
        typer.echo(f"Could not add paper: {exc}")
        log.error("Failed to add paper '%s': %s", title, exc)
