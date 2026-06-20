"""Add subcommand for pdbsearch.

Registered as ``pdbsearch add`` in :mod:`paper_sorts.cli.app`.
"""

from __future__ import annotations

import logging
from typing import Annotated

import typer

from paper_sorts.cli.prompts import ask_confirm, ask_str
from paper_sorts.db.repositories import PaperCreate
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)

app = typer.Typer(help="Add a new paper to the database.")


@app.callback(invoke_without_command=True)
def add_cmd(
    ctx: typer.Context,
    bib_file: Annotated[
        str | None,
        typer.Option("--bib-file", help="Path to a .bib file with the BibTeX entry"),
    ] = None,
) -> None:
    """Interactively add a new paper to the database.

    :param ctx: Typer context carrying the SQLAlchemy engine.
    :param bib_file: Optional path to a ``.bib`` file.  If omitted, the
        BibTeX entry is collected from the user inline.
    """
    engine = ctx.obj["engine"]

    authors_raw = ask_str(
        "Author(s) — comma-separated list (e.g. Smith, John, Doe, Jane): "
    )
    authors = [a.strip() for a in authors_raw.split(",") if a.strip()]
    if not authors:
        print("At least one author is required.")
        return

    title = ask_str("Paper title: ")
    bibtex_key = ask_str("BibTeX key: ")
    summary = ask_str("One-sentence summary: ")

    if bib_file is not None:
        try:
            with open(bib_file, encoding="utf-8") as f:
                bibtex_text = f.read()
        except OSError as exc:
            print(f"Could not read bib file: {exc}")
            logger.error("Could not read bib file %r: %s", bib_file, exc)
            return
    else:
        bibtex_text = ask_str("Full BibTeX entry: ")

    paper = PaperCreate(
        title=title,
        contents=summary,
        bibtex_id=bibtex_key,
        bibtex=bibtex_text,
        authors=authors,
    )

    print(
        f"\nAbout to add:\n"
        f"  Title:   {title}\n"
        f"  Authors: {', '.join(authors)}\n"
        f"  Key:     {bibtex_key}\n"
    )
    if not ask_confirm("Proceed with adding this entry?"):
        print("Aborted — no changes made.")
        return

    try:
        paper_service.add_paper(engine, paper)
        print(f"Added {bibtex_key!r} to the database.")
    except ValueError as exc:
        logger.error("Failed to add paper: %s", exc)
        print(f"Could not add entry: {exc}")
