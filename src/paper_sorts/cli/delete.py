"""Interactive ``delete`` flow (presentation layer)."""

from __future__ import annotations

import logging

from paper_sorts.cli import prompts
from paper_sorts.cli.search import _pick
from paper_sorts.services.paper_service import PaperService

_logger = logging.getLogger(__name__)


def run_delete(service: PaperService) -> bool:
    """Locate a paper by title, confirm, and delete it (links, paper, bib).

    :param service: the bound paper service.
    :returns: ``True`` if a paper was deleted, ``False`` otherwise.
    """
    title = prompts.ask_text("Title of the paper to delete: ")
    papers = service.search_by_title(title.strip())
    if not papers:
        prompts.info("Paper was not found in the database.")
        return False
    chosen = _pick(papers)
    if chosen is None:
        return False
    summary = f"Delete paper '{chosen.title}' ({chosen.bibtex_id}) and its bib entry?"
    if not prompts.confirm(summary):
        prompts.info("Stopping delete process...")
        return False
    try:
        service.delete_paper(chosen.paper_id)
    except ValueError as exc:
        _logger.error("delete failed: %s", exc)
        prompts.info("Could not delete the entry — please check the logs.")
        return False
    prompts.info(f"Deleted '{chosen.title}'.")
    return True
