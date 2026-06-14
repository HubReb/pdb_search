"""add subcommand for pdbsearch CLI.

Provides `pdbsearch add` (direct invocation) and `run_add`
(called from interactive menu). All prompts route through cli/prompts.py.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Annotated

import typer

logger = logging.getLogger(__name__)

app = typer.Typer()


@dataclass
class _PaperInputData:
    """Intermediate data collected from the user before creating a PaperCreate DTO."""

    title: str
    bibtex_id: str
    contents: str
    bibtex: str
    authors: list[str]


def _collect_paper_data(from_bib: str | None = None) -> _PaperInputData:
    """Interactively collect paper metadata from the user.

    :param from_bib: optional path to a .bib file; if given, reads bibtex from it
    :return: _PaperInputData with all required fields populated
    """
    from paper_sorts.cli.prompts import ask_text

    print("Please enter the new paper's information.")
    authors_raw = ask_text("Author(s) — semicolon-separated 'Last, First' names")
    title = ask_text("Paper title")
    bibtex_id = ask_text("BibTeX key (e.g. Wang2021LargeScaleSA)")
    contents = ask_text("Summary / abstract")

    if from_bib:
        try:
            with open(from_bib) as f:
                bibtex = f.read()
        except OSError as exc:
            print(f"Could not read .bib file: {exc}")
            logger.error("Failed to read .bib file '%s': %s", from_bib, exc)
            raise typer.Exit(1) from exc
    else:
        bibtex = ask_text("BibTeX entry (paste full entry)")

    # Authors: split by ";" for multiple authors; whole string if no semicolon.
    if ";" in authors_raw:
        authors = [a.strip() for a in authors_raw.split(";") if a.strip()]
    else:
        authors = [authors_raw.strip()]

    return _PaperInputData(
        title=title,
        bibtex_id=bibtex_id,
        contents=contents,
        bibtex=bibtex,
        authors=authors,
    )


def run_add(database_url: str, from_bib: str | None = None) -> None:
    """Interactive add flow (called from the main menu).

    :param database_url: PostgreSQL DSN
    :param from_bib: optional path to a .bib file
    """
    from paper_sorts.cli.prompts import ask_confirm
    from paper_sorts.db.repositories import PaperCreate
    from paper_sorts.services.paper_service import add_paper

    data = _collect_paper_data(from_bib)
    print(
        f"\nReady to add:\n"
        f"  Title  : {data.title}\n"
        f"  Authors: {', '.join(data.authors)}\n"
        f"  BibTeX : {data.bibtex_id}\n"
    )
    if not ask_confirm("Confirm add?"):
        print("Add cancelled.")
        return

    paper_create = PaperCreate(
        title=data.title,
        contents=data.contents,
        bibtex_id=data.bibtex_id,
        bibtex=data.bibtex,
        authors=data.authors,
    )
    try:
        result = add_paper(database_url, paper_create)
        print(f"Added '{result.title}'.")
    except ValueError as exc:
        print(f"Could not add paper: {exc}")
        logger.error("add_paper failed: %s", exc)
    except Exception as exc:
        logger.error("Unexpected error adding paper: %s", exc)
        print("Add failed — please check the logs.")


@app.command("add")
def add_cmd(
    ctx: typer.Context,
    from_bib: Annotated[
        str | None,
        typer.Option("--from-bib", help="Path to a .bib file to read BibTeX from"),
    ] = None,
) -> None:
    """Add a new paper entry to the database.

    :param ctx: Typer context carrying settings from the app callback
    :param from_bib: optional path to a .bib file; prompts for BibTeX inline if omitted
    """
    settings = ctx.obj["settings"] if ctx.obj else None
    database_url: str
    if settings is not None:
        database_url = settings.get_database_url()
    else:
        raise typer.BadParameter("No database URL configured.")

    run_add(database_url, from_bib)
