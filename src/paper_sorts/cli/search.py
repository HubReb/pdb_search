"""Interactive ``search`` flow: by author or by paper title.

Output reproduces the legacy "pretty print": title, authors (``" and "``-joined),
summary, and the BibTeX entry. A not-found result is a plain message; technical
detail (if any) goes to the logger, never to stdout.
"""

from __future__ import annotations

import logging

from rich.console import Console

from paper_sorts.cli.prompts import ABORT, ask_choice, ask_text
from paper_sorts.db.repositories import PaperSummary
from paper_sorts.db.session import DbEngine
from paper_sorts.services import paper_service

_logger = logging.getLogger(__name__)
_console = Console()


def pretty_print(summary: PaperSummary) -> None:
    """Print one paper in the legacy pretty-print layout.

    :param summary: the paper to display.
    """
    _console.print(f"title: {summary.title}")
    _console.print(f"authors: {summary.authors}")
    _console.print(f"summary: {summary.contents}")
    _console.print(f"bib entry: {summary.bibtex}")
    _console.print("")


def _pick(results: list[PaperSummary]) -> PaperSummary | None:
    """Return a single chosen paper, disambiguating if several match.

    :param results: the matching papers.
    :return: the chosen paper, or ``None`` if the user aborted.
    """
    if len(results) == 1:
        return results[0]
    labels = [f"{r.title} ({r.bibtex_id})" for r in results]
    choice = ask_choice("Multiple papers match — choose one:", labels)
    if isinstance(choice, str):
        return None
    return results[choice]


def run_search(engine: DbEngine) -> None:
    """Drive the interactive search dialog.

    :param engine: the database engine.
    """
    choice = ask_choice(
        "How do you want to search?",
        ["Search by author", "Search by paper title"],
    )
    if choice == ABORT:
        return

    if choice == 0:
        author = ask_text("Author name (Last, First)")
        results = paper_service.search_by_author(engine, author)
        if not results:
            _console.print("No papers found for that author.")
            _logger.info("search_by_author: no results for %r", author)
            return
        for summary in results:
            pretty_print(summary)
        return

    title = ask_text("Paper title")
    results = paper_service.search_by_title(engine, title)
    if not results:
        _console.print("No papers found with that title.")
        _logger.info("search_by_title: no results for %r", title)
        return
    chosen = _pick(results)
    if chosen is not None:
        pretty_print(chosen)
