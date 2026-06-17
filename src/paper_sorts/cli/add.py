"""Interactive ``add`` flow: build a :class:`PaperCreate` and persist it.

Prompts re-prompt on empty input (legacy ``get_user_input`` behaviour). The
BibTeX entry can be read from a file or typed inline. A duplicate BibTeX key
produces a plain message; the technical detail goes to the logger.
"""

from __future__ import annotations

import logging
from pathlib import Path

from rich.console import Console
from sqlalchemy import Engine

from paper_sorts.cli.prompts import ABORT, ask_choice, ask_text
from paper_sorts.db.repositories import DuplicateBibtexError, PaperCreate
from paper_sorts.services import paper_service

_logger = logging.getLogger(__name__)
_console = Console()


def _read_bibtex() -> str | None:
    """Read the BibTeX source from a file or inline input.

    :return: the BibTeX source, or ``None`` if the user aborted.
    """
    choice = ask_choice(
        "Read the BibTeX entry from a file?",
        ["Yes (from a file)", "No (type it inline)"],
    )
    if choice == ABORT:
        return None
    if choice == 0:
        while True:
            filename = ask_text("Enter filename")
            path = Path(filename)
            if path.is_file():
                return path.read_text(encoding="utf-8")
            _console.print(f"No readable file at {filename!r} — try again.")
    return ask_text("Enter the BibTeX entry")


def run_add(engine: Engine) -> None:
    """Drive the interactive add dialog.

    :param engine: the database engine.
    """
    authors_raw = ask_text("Authors (comma-separated, each 'Last, First')")
    authors = _split_authors(authors_raw)
    title = ask_text("Title")
    bibtex_id = ask_text("BibTeX key")
    bibtex = _read_bibtex()
    if bibtex is None:
        _console.print("Add aborted.")
        return
    summary = ask_text("Summary")

    paper = PaperCreate(
        title=title,
        contents=summary,
        bibtex_id=bibtex_id,
        bibtex=bibtex,
        authors=authors,
    )
    try:
        paper_service.add_paper(engine, paper)
    except DuplicateBibtexError as exc:
        _logger.warning("add_paper failed: %s", exc)
        _console.print("That BibTeX entry already exists — nothing was added.")
        return
    _console.print(f"Added {title!r}.")


def _split_authors(raw: str) -> list[str]:
    """Split a comma-separated author string into ``"Last, First"`` names.

    Author names themselves contain a comma (``"Last, First"``), so names are
    separated on the semantic ``"; "`` only if present; otherwise the raw string
    is paired up two comma-fields at a time.

    :param raw: the user's author input.
    :return: a list of author names.
    """
    if ";" in raw:
        return [part.strip() for part in raw.split(";") if part.strip()]
    fields = [field.strip() for field in raw.split(",") if field.strip()]
    names: list[str] = []
    for i in range(0, len(fields) - 1, 2):
        names.append(f"{fields[i]}, {fields[i + 1]}")
    if len(fields) % 2 == 1:
        names.append(fields[-1])
    return names or [raw.strip()]
