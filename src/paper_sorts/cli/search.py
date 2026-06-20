"""Search subcommand for pdbsearch.

Registered as ``pdbsearch search`` in :mod:`paper_sorts.cli.app`.
"""

from __future__ import annotations

import logging
from typing import Annotated

import typer
from rich.console import Console

from paper_sorts.cli.prompts import ask_choice, ask_str
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)
console = Console()

app = typer.Typer(help="Search the paper database by title or author.")


def _display_paper(summary: paper_sorts.db.repositories.PaperSummary) -> None:  # type: ignore[name-defined]  # noqa: F821
    """Print a paper summary to the console."""

    authors_str = " and ".join(summary.authors)
    console.print(f"[bold]Title:[/bold] {summary.title}")
    console.print(f"[bold]Authors:[/bold] {authors_str}")
    console.print(f"[bold]Summary:[/bold] {summary.contents}")
    console.print(f"[bold]BibTeX:[/bold]\n{summary.bibtex}")


@app.callback(invoke_without_command=True)
def search_cmd(
    ctx: typer.Context,
    author: Annotated[str | None, typer.Option("--author", help="Search by author name")] = None,
    title: Annotated[str | None, typer.Option("--title", help="Search by paper title")] = None,
) -> None:
    """Search the database by author name or paper title.

    When called without flags, presents an interactive menu.

    :param ctx: Typer context carrying the SQLAlchemy engine.
    :param author: Author name to search for (optional).
    :param title: Paper title to search for (optional).
    """
    engine = ctx.obj["engine"]

    if author is not None:
        _do_author_search(engine, author)
        return

    if title is not None:
        _do_title_search(engine, title)
        return

    # Interactive mode
    choice = ask_choice(
        ["Search by author", "Search by title", "(Q)uit / abort"],
        prompt="Your choice: ",
    )
    if choice == 2:
        return
    if choice == 0:
        name = ask_str("Please enter the author's name: ")
        _do_author_search(engine, name)
    else:
        t = ask_str("Please enter the paper title: ")
        _do_title_search(engine, t)


def _do_author_search(engine: object, author: str) -> None:
    """Search by author and display results."""
    results = paper_service.search_by_author(engine, author)  # type: ignore[arg-type]
    if not results:
        print("Author not found in database.")
        logger.info("Author %r not found", author)
        return

    if len(results) == 1:
        _display_paper(results[0])
        return

    options = [f"{r.title}" for r in results]
    options.append("(Q)uit / abort")
    idx = ask_choice(options, prompt="Choose paper to display: ")
    if idx < len(results):
        _display_paper(results[idx])


def _do_title_search(engine: object, title: str) -> None:
    """Search by title and display results (with disambiguation if needed)."""
    results = paper_service.search_by_title(engine, title)  # type: ignore[arg-type]
    if not results:
        print("Paper not found in database.")
        logger.info("Title %r not found", title)
        return

    if len(results) == 1:
        _display_paper(results[0])
        return

    options = [f"{r.title} [{r.bibtex_id}]" for r in results]
    options.append("(Q)uit / abort")
    idx = ask_choice(options, prompt="Choose paper to display: ")
    if idx < len(results):
        _display_paper(results[idx])
