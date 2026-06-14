"""Typer `add` subcommand for paper_sorts.

Prompts the user for all required paper fields and inserts into the database.
All prompts route through cli/prompts.py (constitution III).
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import typer
from sqlalchemy import Engine

from paper_sorts.cli import prompts
from paper_sorts.db.repositories import PaperCreate
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service

logger = logging.getLogger("paper_sorts.cli.add")

app = typer.Typer()


def run_add(engine: Engine) -> bool:
    """Run the interactive add-paper dialog against the given engine.

    :param engine: SQLAlchemy engine connected to the database
    :type engine: Engine
    :return: True if paper was added successfully, False otherwise
    :rtype: bool
    """
    author_str = prompts.ask_text(
        "Author(s) — comma-separated 'Last, First' list"
    )
    paper_title = prompts.ask_text("Paper title")
    bibtex_key = prompts.ask_text("BibTeX key")

    bibtex_source = prompts.ask_bibtex_source()
    if bibtex_source is None:
        print("Add cancelled.")
        return False

    if bibtex_source == "file":
        bibtex_file = prompts.ask_text("Enter path to .bib file")
        bib_path = Path(bibtex_file.strip())
        if not bib_path.exists():
            print(f"File not found: {bib_path}")
            logger.error("BibTeX file not found: %s", bib_path)
            return False
        bibtex_information = bib_path.read_text(encoding="utf-8")
    else:
        bibtex_information = prompts.ask_text("BibTeX entry")

    contents = prompts.ask_text("Summary of the paper")

    # Parse authors
    authors = [a.strip() for a in author_str.split(",") if a.strip()]
    if not authors:
        print("At least one author is required.")
        return False

    paper_data = PaperCreate(
        title=paper_title,
        contents=contents,
        bibtex_id=bibtex_key,
        bibtex=bibtex_information,
        authors=authors,
    )

    try:
        with with_session(engine) as session:
            summary = paper_service.add_paper(session, paper_data)
        print(f"Added: {summary.title} (key: {summary.bibtex_id})")
        logger.info("Added paper '%s' with key '%s'", summary.title, summary.bibtex_id)
        return True
    except ValueError as exc:
        print(f"Could not add paper: {exc}")
        logger.error("add_paper failed: %s", exc)
        return False
    except Exception as exc:
        print("Could not add paper. Check logs for details.")
        logger.error("add_paper unexpected error: %s", exc)
        return False


@app.command("add")
def add_command(
    ctx: typer.Context,
) -> None:
    """Add a new paper entry to the database."""
    engine: Engine = ctx.obj
    success = run_add(engine)
    if not success:
        sys.exit(1)
