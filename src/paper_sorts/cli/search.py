"""The ``search`` subcommand: find a paper by author or by title."""

from __future__ import annotations

from paper_sorts.cli import prompts
from paper_sorts.db.repositories import PaperSummary
from paper_sorts.services.paper_service import PaperService


def pick_paper(papers: list[PaperSummary]) -> PaperSummary | None:
    """Disambiguate among multiple paper hits, or return the sole hit.

    :param papers: the candidate papers (at least one).
    :return: the chosen paper, or ``None`` if the user aborts.
    """
    if len(papers) == 1:
        return papers[0]
    labels = [f"title: {p.title} — authors: {' and '.join(p.authors)}" for p in papers]
    choice = prompts.ask_choice("Following papers found:", labels)
    if choice is None:
        return None
    return papers[choice]


def run_search(service: PaperService) -> None:
    """Run the interactive search dialog (by author or by title).

    :param service: the paper service to query.
    """
    choice = prompts.ask_choice(
        "Search interface — please choose a method:",
        ["Search by author", "Search by paper title"],
    )
    if choice is None:
        return
    if choice == 0:
        author = prompts.ask_text("Please enter the author's name")
        papers = service.search_by_author(author)
        not_found = "Author was not found in database."
    else:
        title = prompts.ask_text("Please enter the paper title")
        papers = service.search_by_title(title)
        not_found = "Paper was not found in database."
    if not papers:
        prompts.error(not_found)
        return
    chosen = pick_paper(papers)
    if chosen is not None:
        prompts.print_paper(chosen)
