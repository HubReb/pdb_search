"""Interactive ``search`` flow (presentation layer)."""

from __future__ import annotations

from paper_sorts.cli import prompts
from paper_sorts.db.repositories import PaperSummary
from paper_sorts.services.paper_service import PaperService


def _pick(papers: list[PaperSummary]) -> PaperSummary | None:
    """Return the single paper, or disambiguate a multi-match list."""
    if len(papers) == 1:
        return papers[0]
    index = prompts.ask_choice(
        "Following papers found:",
        [f"title: {p.title}" for p in papers],
    )
    return papers[index] if index is not None else None


def run_search(service: PaperService) -> None:
    """Drive the interactive search dialog.

    :param service: the bound paper service.
    """
    choice = prompts.ask_choice(
        "Search interface — please choose a method:",
        ["Search by author", "Search by paper title"],
    )
    if choice is None:
        return
    if choice == 0:
        author = prompts.ask_text("Please enter the author's name: ")
        papers = service.search_by_author(author.strip())
        if not papers:
            prompts.info("Author was not found in the database.")
            return
    else:
        title = prompts.ask_text("Please enter the paper title: ")
        papers = service.search_by_title(title.strip())
        if not papers:
            prompts.info("Paper was not found in the database.")
            return
    chosen = _pick(papers)
    if chosen is not None:
        prompts.print_paper(chosen)
