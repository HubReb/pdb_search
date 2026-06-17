"""Interactive search flow: by author or paper title, with disambiguation."""

from __future__ import annotations

from rich.console import Console
from sqlalchemy import Engine

from paper_sorts.cli.prompts import ask_choice, ask_nonempty, ask_pick
from paper_sorts.db.repositories import PaperSummary
from paper_sorts.logging_config import get_logger
from paper_sorts.services.paper_service import PaperService

console = Console()
logger = get_logger()


def _render(summary: PaperSummary) -> None:
    """Print a resolved paper in the legacy ``pretty_print`` layout.

    :param summary: the paper to display.
    """
    console.print(f"title: {summary.title}")
    console.print(f"authors: {summary.authors}")
    console.print(f"summary: {summary.contents}")
    console.print(f"bib entry: {summary.bibtex or ''}")


def _disambiguate(results: list[PaperSummary]) -> PaperSummary | None:
    """Pick one paper from multiple matches, or abort.

    :param results: the candidate papers.
    :returns: the chosen paper, or ``None`` if the user aborts.
    """
    if len(results) == 1:
        return results[0]
    labels = [f"title: {r.title} ({r.bibtex_id})" for r in results]
    index = ask_pick("Following papers found:", labels)
    if index is None:
        return None
    return results[index]


def run_search(engine: Engine) -> None:
    """Run the search sub-menu against the database.

    :param engine: the engine bound to the configured database.
    """
    service = PaperService(engine)
    choice = ask_choice(
        "Search interface\nPlease choose a method:",
        ["Search by author", "Search by paper title"],
    )
    if choice is None:
        return
    if choice == 0:
        author = ask_nonempty("Please enter the author's name")
        results = service.search_by_author(author)
        if not results:
            console.print("Author was not found in the database.")
            return
    else:
        title = ask_nonempty("Please enter the paper title")
        results = service.search_by_title(title)
        if not results:
            console.print("Paper was not found in the database.")
            return
    chosen = _disambiguate(results)
    if chosen is None:
        return
    _render(chosen)
