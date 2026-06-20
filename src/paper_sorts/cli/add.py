"""Add subcommand for pdbsearch CLI."""

import logging
import sys
from pathlib import Path

import typer
from sqlalchemy.engine import Engine

from paper_sorts.cli.prompts import ask_choice, ask_confirmation, ask_input
from paper_sorts.db.repositories import PaperCreate
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)

app = typer.Typer(help="Add a new paper to the database.")


def run_add(engine: Engine) -> None:
    """Interactive add flow — called from the top-level menu.

    :param engine: Active SQLAlchemy engine.
    """
    # Collect paper data
    authors_raw = ask_input(
        "Author(s) — comma-separated list in 'Last, First' form: "
    )
    title = ask_input("Paper title: ")
    bibtex_key = ask_input("BibTeX key: ")

    # BibTeX source
    print("How do you want to provide the BibTeX entry?")
    src_idx = ask_choice(
        ["From a .bib file", "Enter inline"],
        prompt="Your choice: ",
        quit_label="(A)bort",
    )
    if src_idx == -1:
        print("Stopping add process.")
        return

    if src_idx == 0:
        bib_file = ask_input("Enter .bib filename: ")
        try:
            bibtex_text = Path(bib_file).read_text(encoding="utf-8")
        except OSError as exc:
            logger.error("Could not read bib file '%s': %s", bib_file, exc)
            print(f"Could not read '{bib_file}': {exc}")
            return
    else:
        bibtex_text = ask_input("Enter BibTeX entry: ")

    summary = ask_input("One-sentence summary: ")
    authors = [a.strip() for a in authors_raw.split(",") if a.strip()]
    if not authors:
        print("No authors provided — aborting.")
        return

    paper = PaperCreate(
        title=title,
        contents=summary,
        bibtex_id=bibtex_key,
        authors=authors,
        bibtex=bibtex_text,
    )

    if not ask_confirmation(
        f"About to add:\n  Title: {title}\n  BibTeX key: {bibtex_key}\n"
        f"  Authors: {', '.join(authors)}"
    ):
        print("Add aborted.")
        return

    try:
        paper_service.add_paper(engine, paper)
        print(f"Successfully added '{title}'.")
    except ValueError as exc:
        logger.error("Failed to add paper: %s", exc)
        print(f"Could not add paper: {exc}")


@app.command()
def add_cmd(
    ctx: typer.Context,
    bib_file: Path | None = typer.Option(None, "--bib-file", help="Path to .bib file"),
) -> None:
    """Add a new paper, optionally reading BibTeX from a file.

    :param ctx: Typer context carrying the engine.
    :param bib_file: Optional path to a ``.bib`` file.
    """
    raw_engine = ctx.obj.get("engine") if ctx.obj else None
    if raw_engine is None or not isinstance(raw_engine, Engine):
        logger.error("No database connection available")
        print("Error: no database URL configured.")
        sys.exit(1)

    run_add(raw_engine)
