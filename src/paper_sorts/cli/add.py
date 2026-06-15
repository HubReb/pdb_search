"""The ``add`` command: add a new paper, inline or from a ``.bib`` file."""

from __future__ import annotations

from pathlib import Path

from pybtex.database import parse_string
from sqlalchemy import Engine

from paper_sorts.cli import prompts
from paper_sorts.db.repositories import PaperCreate
from paper_sorts.services import paper_service


def parse_single_bib(source: str) -> tuple[str, list[str], str, str]:
    """Parse a single-entry BibTeX string.

    :param source: the BibTeX source containing exactly one entry.
    :returns: ``(bibtex_source, authors, bibtex_key, title)``.
    :raises ValueError: if the source does not contain exactly one entry.
    """
    bib = parse_string(source, bib_format="bibtex")
    keys = list(bib.entries.keys())
    if len(keys) != 1:
        raise ValueError("Expected exactly one BibTeX entry")
    key = keys[0]
    entry = bib.entries[key]
    authors = [
        f"{person.last_names[0]}, {person.first_names[0]}"
        for person in entry.persons.get("author", [])
    ]
    title = entry.fields.get("title", "")
    return entry.to_string("bibtex"), authors, key, title


def run_add(engine: Engine, bib_file: str | None = None) -> bool:
    """Drive the interactive add flow.

    Prompts for author list, title, BibTeX key, the BibTeX source (inline or
    from a file), and a summary, then persists the paper.

    :param engine: the database engine.
    :param bib_file: optional path to a single-entry ``.bib`` file; when given,
        the BibTeX source is read from it instead of prompting.
    :returns: ``True`` on success, ``False`` on failure.
    """
    author = prompts.ask_nonempty("Author(s), please provide a , separated list")
    paper_title = prompts.ask_nonempty("Paper title")
    bibtex_key = prompts.ask_nonempty("bibtex key")
    if bib_file is not None:
        bibtex_source = Path(bib_file).read_text(encoding="utf-8")
    else:
        choice = prompts.ask_choice(
            "Do you want to enter the bibtex entry via a separate file?",
            ["Yes", "No"],
        )
        if choice is None:
            return False
        if choice == 0:
            filename = prompts.ask_nonempty("Enter filename")
            bibtex_source = Path(filename).read_text(encoding="utf-8")
        else:
            bibtex_source = prompts.ask_nonempty("bib entry")
    summary = prompts.ask_nonempty("summary of the paper")
    authors = [name.strip() for name in author.split(",")]
    paper = PaperCreate(
        title=paper_title,
        summary=summary,
        authors=authors,
        bibtex_id=bibtex_key,
        bibtex=bibtex_source,
    )
    try:
        paper_service.add_paper(engine, paper)
    except ValueError as exc:
        prompts.info(f"Could not add entry: {exc}")
        return False
    prompts.info(f"Added entry {', '.join(authors)}: {paper_title}")
    return True
