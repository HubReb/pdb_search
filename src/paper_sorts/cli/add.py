"""Interactive ``add`` flow (presentation layer)."""

from __future__ import annotations

import logging
from pathlib import Path

from paper_sorts.cli import prompts
from paper_sorts.db.repositories import PaperCreate
from paper_sorts.services.paper_service import PaperService

_logger = logging.getLogger(__name__)


def run_add(service: PaperService) -> bool:
    """Prompt for a new paper and persist it.

    :param service: the bound paper service.
    :returns: ``True`` on success, ``False`` on a handled failure.
    """
    author_csv = prompts.ask_text("Author(s), please provide a comma-separated list: ")
    title = prompts.ask_text("Paper title: ")
    bibtex_key = prompts.ask_text("bibtex key: ")
    via_file = prompts.ask_choice(
        "Enter the bibtex entry via a separate file?",
        ["Yes", "No"],
    )
    if via_file is None:
        return False
    if via_file == 0:
        filename = prompts.ask_text("Enter filename: ")
        try:
            bibtex = Path(filename).read_text(encoding="utf-8")
        except OSError as exc:
            _logger.error("could not read bibtex file: %s", exc)
            prompts.info("Could not read that file — please check the path.")
            return False
    else:
        bibtex = prompts.ask_text("bib entry: ")
    summary = prompts.ask_text("summary of the paper: ")

    authors = [a.strip() for a in author_csv.split(",") if a.strip()]
    paper = PaperCreate(
        title=title, summary=summary, bibtex_id=bibtex_key, bibtex=bibtex, authors=authors
    )
    try:
        service.add_paper(paper)
    except ValueError as exc:
        _logger.error("add failed: %s", exc)
        prompts.info("Could not add the entry — please check the logs.")
        return False
    prompts.info(f"Added entry {', '.join(authors)}: {title}")
    return True
