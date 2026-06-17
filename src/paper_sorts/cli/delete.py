"""Interactive ``delete`` flow: identify a paper, summarise it, confirm, delete.

A paper is found by title (disambiguating if several share it), summarised, and
deleted only after an explicit confirmation. Deletion removes the authorship
links, any orphaned authors, and the paper and bib rows.
"""

from __future__ import annotations

import logging

from rich.console import Console

from paper_sorts.cli.prompts import ask_choice, ask_text, confirm
from paper_sorts.db.repositories import PaperNotFoundError, PaperSummary
from paper_sorts.db.session import DbEngine
from paper_sorts.services import paper_service

_logger = logging.getLogger(__name__)
_console = Console()


def _identify(engine: DbEngine) -> PaperSummary | None:
    """Locate the paper to delete by title.

    :param engine: the database engine.
    :return: the chosen paper, or ``None`` if not found or aborted.
    """
    title = ask_text("Title of the paper to delete")
    results = paper_service.search_by_title(engine, title)
    if not results:
        _console.print("No papers found with that title.")
        return None
    if len(results) == 1:
        return results[0]
    labels = [f"{r.title} ({r.bibtex_id})" for r in results]
    choice = ask_choice("Multiple papers match — choose one to delete:", labels)
    if isinstance(choice, str):
        return None
    return results[choice]


def run_delete(engine: DbEngine) -> None:
    """Drive the interactive delete dialog.

    :param engine: the database engine.
    """
    target = _identify(engine)
    if target is None:
        return

    _console.print(f"title: {target.title}")
    _console.print(f"authors: {target.authors}")
    _console.print(f"bib entry: {target.bibtex_id}")
    if not confirm("Delete this paper?"):
        _console.print("Nothing deleted.")
        return

    try:
        paper_service.delete_paper(engine, target.paper_id)
    except PaperNotFoundError as exc:
        _logger.warning("delete_paper failed: %s", exc)
        _console.print("Could not delete that paper.")
        return
    _console.print(f"Deleted {target.title!r}.")
