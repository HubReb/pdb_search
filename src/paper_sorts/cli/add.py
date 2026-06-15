"""CLI 'add' subcommand for paper_sorts.

Prompts the user for all paper metadata and optionally reads the BibTeX entry
from a file. All prompts route through cli/prompts.py (constitution Principle III).
"""

from __future__ import annotations

import logging
from pathlib import Path

import typer

from paper_sorts.cli.prompts import ask_choice, ask_nonempty
from paper_sorts.db.repositories import PaperCreate
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)

app = typer.Typer()


def run_add(db_url: str) -> None:
    """Execute the interactive add flow.

    :param db_url: SQLAlchemy-compatible database URL.
    """
    author_raw = ask_nonempty(
        "Please enter the necessary information\nAuthor(s) — comma-separated list"
    )
    authors = [a.strip() for a in author_raw.split(",") if a.strip()]

    title = ask_nonempty("Paper title")
    bibtex_id = ask_nonempty("BibTeX key")

    bib_source_idx = ask_choice(
        "Do you want to enter the BibTeX entry via a separate file?",
        ["Yes — enter from file", "No — enter inline"],
    )
    if bib_source_idx == 0:
        bib_file = ask_nonempty("Enter the path to the .bib file")
        bib_path = Path(bib_file)
        if not bib_path.exists():
            typer.echo(f"File not found: {bib_path}", err=True)
            logger.error("BibTeX file not found: %s", bib_path)
            return
        bibtex = bib_path.read_text(encoding="utf-8")
    else:
        bibtex = ask_nonempty("Enter the BibTeX entry")

    contents = ask_nonempty("Summary of the paper (one sentence)")

    paper = PaperCreate(
        title=title,
        contents=contents,
        bibtex_id=bibtex_id,
        bibtex=bibtex,
        authors=authors,
    )

    with with_session(db_url) as session:
        success = paper_service.add_paper(session, paper)

    if success:
        typer.echo(f"Paper '{title}' added successfully.")
    else:
        typer.echo(
            f"Could not add paper '{title}' — it may already exist. Check logs for details.",
            err=True,
        )


@app.command()
def add(ctx: typer.Context) -> None:
    """Add a new paper entry to the database."""
    db_url: str = ctx.obj["db_url"]
    run_add(db_url)
