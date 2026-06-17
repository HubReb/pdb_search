"""Interactive add flow: gather paper metadata and persist a new entry."""

from __future__ import annotations

from rich.console import Console
from sqlalchemy import Engine

from paper_sorts.cli.prompts import ask_choice, ask_nonempty
from paper_sorts.db.repositories import DuplicateError, PaperCreate
from paper_sorts.logging_config import get_logger
from paper_sorts.services.paper_service import PaperService

console = Console()
logger = get_logger()


def run_add(engine: Engine) -> bool:
    """Prompt for a new paper's fields and add it to the database.

    :param engine: the engine bound to the configured database.
    :returns: ``True`` on success, ``False`` on a handled failure.
    """
    authors_raw = ask_nonempty("Author(s) — ';'-separated list of 'Last, First' names")
    title = ask_nonempty("Paper title")
    bibtex_key = ask_nonempty("bibtex key")

    bib_choice = ask_choice("Enter the bibtex entry via a separate file?", ["Yes", "No"])
    if bib_choice is None:
        console.print("Add aborted.")
        return False
    if bib_choice == 0:
        filename = ask_nonempty("Enter filename")
        try:
            with open(filename, encoding="utf-8") as handle:
                bibtex = handle.read()
        except OSError as exc:
            logger.error("could not read bib file %s: %s", filename, exc)
            console.print("Could not read the bib file - please check the path.")
            return False
    else:
        bibtex = ask_nonempty("bib entry")

    summary = ask_nonempty("summary of the paper")
    # Authors are entered as a ", "-separated list of "Last, First" names; the
    # original prompt asks for a comma-separated list. A single author entered
    # as "Last, First" is one author, so split on a "; " delimiter between
    # authors to keep "Last, First" intact while still supporting multiples.
    authors = [a.strip() for a in authors_raw.split(";") if a.strip()]

    try:
        PaperService(engine).add_paper(
            PaperCreate(
                title=title,
                contents=summary,
                bibtex_id=bibtex_key,
                bibtex=bibtex,
                authors=authors,
            )
        )
    except DuplicateError as exc:
        logger.info("add rejected: %s", exc)
        console.print("Could not add entry: that bibtex key already exists.")
        return False
    except ValueError as exc:
        logger.error("add failed: %s", exc)
        console.print("Could not add entry - please check logs.")
        return False

    logger.info("added entry %s: %s", ", ".join(authors), title)
    console.print(f"Added '{title}'.")
    return True
