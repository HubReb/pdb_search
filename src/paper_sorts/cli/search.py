"""CLI 'search' subcommand for paper_sorts.

Prompts the user for a search method (by author or by title), then a search term,
and displays the matching paper(s). When multiple papers match, the user is
prompted to choose one.
"""

from __future__ import annotations

import logging

import typer

from paper_sorts.cli.prompts import ask_choice, ask_nonempty
from paper_sorts.db.repositories import PaperSummary
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)

app = typer.Typer()


def _display_paper(paper: PaperSummary) -> None:
    """Pretty-print a single paper to stdout.

    :param paper: PaperSummary DTO to display.
    """
    authors = ", ".join(paper.authors) if paper.authors else "(none)"
    typer.echo(f"\ntitle: {paper.title}")
    typer.echo(f"authors: {authors}")
    typer.echo(f"summary: {paper.contents}")
    typer.echo(f"bib entry:\n{paper.bibtex}")


def _pick_paper(results: list[PaperSummary]) -> PaperSummary:
    """Ask the user to choose one paper from a list.

    :param results: Non-empty list of PaperSummary DTOs.
    :return: The chosen PaperSummary.
    """
    options = [f"{p.title} [{p.bibtex_id}]" for p in results]
    idx = ask_choice("Following papers found. Choose one:", options)
    return results[idx]


def run_search(db_url: str) -> None:
    """Execute the interactive search flow.

    :param db_url: SQLAlchemy-compatible database URL.
    """
    method_idx = ask_choice(
        "Search interface\nPlease choose a method:",
        ["Search by author", "Search by title", "(Q)uit"],
    )
    if method_idx == 2:  # Quit
        typer.echo("Returning to main menu.")
        return

    with with_session(db_url) as session:
        if method_idx == 0:
            author = ask_nonempty("Please enter the author's name")
            results = paper_service.search_by_author(session, author)
            if not results:
                typer.echo(f"Author '{author}' was not found in the database.")
                return
        else:
            title = ask_nonempty("Please enter the paper title")
            results = paper_service.search_by_title(session, title)
            if not results:
                typer.echo(f"Paper '{title}' was not found in the database.")
                return

        paper = results[0] if len(results) == 1 else _pick_paper(results)
        _display_paper(paper)


@app.command()
def search(ctx: typer.Context) -> None:
    """Search the database by author or title."""
    db_url: str = ctx.obj["db_url"]
    run_search(db_url)
