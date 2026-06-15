"""Interactive delete flow for the CLI.

Identifies the paper (search by title, disambiguating if needed), shows a dual-form
confirmation summarising what will be removed, then deletes the paper, its BibTeX entry, and its
author links via the service.
"""

from __future__ import annotations

import logging

from paper_sorts.cli import prompts
from paper_sorts.services.paper_service import PaperService

logger = logging.getLogger(__name__)


def run_delete(service: PaperService) -> bool:
    """Run the interactive delete dialog.

    :param service: the paper service to delete through.
    :return: ``True`` if a paper was deleted, ``False`` if aborted or not found.
    """
    title = prompts.ask_nonempty("Title of the paper to delete")
    results = service.search_by_title(title.strip())
    if not results:
        prompts.show("Paper was not found in the database.")
        return False
    if len(results) == 1:
        chosen = results[0]
    else:
        labels = [f"title: {s.title} — authors: {s.authors}" for s in results]
        chosen = results[prompts.pick_from("Following papers found:", labels)]

    if not prompts.confirm(
        f"You wish to delete '{chosen.title}' by {chosen.authors} "
        f"(BibTeX key '{chosen.bibtex_id}'). Proceed?"
    ):
        prompts.show("Stopping delete process...")
        return False

    if service.delete_paper(chosen.paper_id):
        prompts.show("Deleted.")
        return True
    prompts.show("Could not delete the entry - please check the logs.")
    return False
