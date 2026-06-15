"""Interactive search flow for the CLI.

Routes all input through ``cli/prompts`` and renders the legacy pretty-print record. Handles
the by-author / by-title sub-menu, disambiguation on multiple title matches, and plain
"not found" messaging.
"""

from __future__ import annotations

import logging

from paper_sorts.cli import prompts
from paper_sorts.db.repositories import PaperSummary
from paper_sorts.services.paper_service import PaperService

logger = logging.getLogger(__name__)


def _display(summary: PaperSummary) -> None:
    """Render one paper in the legacy pretty-print format.

    :param summary: the paper to display.
    """
    prompts.show(f"title: {summary.title}")
    prompts.show(f"authors: {summary.authors}")
    prompts.show(f"summary: {summary.summary}")
    prompts.show(f"bib entry: {summary.bibtex}")


def _pick(summaries: list[PaperSummary]) -> PaperSummary:
    """Return a single summary, disambiguating if there is more than one.

    :param summaries: the candidate summaries (at least one).
    :return: the chosen summary.
    """
    if len(summaries) == 1:
        return summaries[0]
    labels = [f"title: {s.title} — authors: {s.authors}" for s in summaries]
    index = prompts.pick_from("Following papers found:", labels)
    return summaries[index]


def run_search(service: PaperService) -> None:
    """Run the interactive search sub-menu.

    :param service: the paper service to query.
    """
    choice = prompts.ask_choice(
        "Search interface",
        ["Search by author", "Search by paper title", "abort"],
    )
    if choice == 0:
        author = prompts.ask_nonempty("Please enter the author's name (Last, First)")
        results = service.search_by_author(author.strip())
        not_found = "Author was not found in the database."
    elif choice == 1:
        title = prompts.ask_nonempty("Please enter the paper title")
        results = service.search_by_title(title.strip())
        not_found = "Paper was not found in the database."
    else:
        return

    if not results:
        prompts.show(not_found)
        return
    _display(_pick(results))
