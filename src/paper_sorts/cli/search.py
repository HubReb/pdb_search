"""Search subcommand for pdbsearch CLI.

Provides interactive search by author or by paper title, with
disambiguation when multiple results are found.
"""

from __future__ import annotations

import logging

from rich.console import Console

from paper_sorts.cli.prompts import ask_choice, ask_text
from paper_sorts.db.repositories import PaperSummary
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)
console = Console()


def _pretty_print(paper: PaperSummary) -> None:
    """Print a paper record in the legacy pretty-print format.

    Args:
        paper: PaperSummary DTO to display.
    """
    console.print(f"title: {paper.title}")
    console.print(f"authors: {paper.authors}")
    console.print(f"summary: {paper.contents}")
    console.print(f"bib entry: {paper.bibtex}")


def _disambiguate(papers: list[PaperSummary]) -> PaperSummary:
    """Show a numbered list and let the user pick one paper.

    Args:
        papers: Non-empty list of PaperSummary DTOs.

    Returns:
        The user-selected PaperSummary.
    """
    options = [f"{p.title} (id={p.paper_id})" for p in papers]
    idx = ask_choice("Following papers found — please choose one:", options)
    return papers[idx]


def search_callback(db_url: str) -> None:
    """Interactive search subcommand entrypoint.

    Asks the user to pick between author-search and title-search, then
    prompts for the search term and displays results.

    Args:
        db_url: SQLAlchemy-compatible database URL from the app callback.
    """
    options = ["Search by author", "Search by paper title", "Abort"]
    idx = ask_choice("Search interface\nPlease choose a method:", options)

    if idx == 2:  # Abort
        console.print("Search aborted.")
        return

    if idx == 0:
        _search_by_author(db_url)
    else:
        _search_by_title(db_url)


def _search_by_author(db_url: str) -> None:
    """Prompt for author name and display matching papers.

    Args:
        db_url: SQLAlchemy-compatible database URL.
    """
    author = ask_text("Please enter the author's name: ")
    papers = paper_service.search_by_author(db_url, author.strip())
    if not papers:
        console.print("Author was not found in database.")
        logger.info("Author %r not found.", author)
        return

    paper = _disambiguate(papers) if len(papers) > 1 else papers[0]
    _pretty_print(paper)


def _search_by_title(db_url: str) -> None:
    """Prompt for paper title and display the matching paper.

    Args:
        db_url: SQLAlchemy-compatible database URL.
    """
    title = ask_text("Please enter the paper title: ")
    papers = paper_service.search_by_title(db_url, title.strip())
    if not papers:
        console.print("Paper was not found in database.")
        logger.info("Title %r not found.", title)
        return

    paper = _disambiguate(papers) if len(papers) > 1 else papers[0]
    _pretty_print(paper)
