"""``pdbsearch add`` — add a paper to the database.

Supports the non-interactive form via ``--bib-file`` and ``--summary``
flags (which skip the corresponding prompts) or the fully interactive
sequence preserved verbatim from ``UserInteraction.add``:

* authors as a comma-separated list
* paper title
* BibTeX key
* BibTeX source (paste inline, or read from a file)
* summary

The whole insert is atomic — a single SQLAlchemy transaction wraps the
bib + paper + author rows + authors_papers links. ``BibTeX key
uniqueness is enforced before any insert`` (per the CLI command
contract): the service pre-checks and raises
:class:`DuplicateBibtexIdError`, which this command catches and renders
as a plain-language error.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from paper_sorts.cli.prompts import ask_choice, ask_text
from paper_sorts.db.repositories import DuplicateBibtexIdError, PaperCreate
from paper_sorts.db.session import with_session
from paper_sorts.services.paper_service import PaperService


def add(
    ctx: typer.Context,
    bib_file: Annotated[
        Path | None,
        typer.Option(
            "--bib-file",
            help="Path to a .bib file holding the BibTeX source string.",
        ),
    ] = None,
    summary: Annotated[
        str | None,
        typer.Option("--summary", help="One-line paper summary."),
    ] = None,
) -> None:
    """Add a paper to the database via prompts (or flags for scripted runs)."""
    payload = _gather_input(bib_file, summary)
    if payload is None:
        return

    factory = ctx.obj
    try:
        with with_session(factory) as session:
            service = PaperService(session)
            inserted = service.add_paper(payload)
        print(f"Added paper id {inserted.id}: {inserted.title}")
    except DuplicateBibtexIdError as e:
        print(f"Error: {e}")


def _gather_input(bib_file: Path | None, summary: str | None) -> PaperCreate | None:
    """Run the prompt sequence; return ``PaperCreate`` or ``None`` on input error."""
    print("Please enter the necessary information")

    # Splits on ", " verbatim from legacy ``UserInteraction.add`` (line 154 of
    # the old user_interaction.py). The legacy never handled comma+space
    # *inside* "Last, First" names cleanly — preserving the quirk because
    # FR-002 forbids changing the prompt's parsing semantics.
    authors_csv = ask_text("Author(s), please provide a , separated list")
    authors = tuple(a for a in authors_csv.split(", ") if a)
    if not authors:
        print("Error: at least one author is required.")
        return None

    title = ask_text("Paper title")
    bibtex_id = ask_text("bibtex key")

    if bib_file is None:
        choice = ask_choice(
            "Do you want to enter the bibtex entry via a separate file?",
            ["Yes", "No"],
        )
        if choice == 1:
            bib_file = Path(ask_text("Enter filename"))

    if bib_file is not None:
        try:
            bibtex = bib_file.read_text(encoding="utf-8")
        except FileNotFoundError:
            print(f"Error: bib file {str(bib_file)!r} does not exist.")
            return None
    else:
        bibtex = ask_text("bib entry")

    if not summary:
        summary = ask_text("summary of the paper_information")

    return PaperCreate(
        title=title,
        contents=summary,
        bibtex_id=bibtex_id,
        bibtex=bibtex,
        authors=authors,
    )
