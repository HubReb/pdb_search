"""Search subcommand for paper_sorts CLI.

Implements ``pdbsearch search``: prompts the user to choose author/title
search, performs the search via the service layer, and displays results.
"""

from __future__ import annotations

import logging

import typer
from rich.console import Console
from sqlalchemy.engine import Engine

from paper_sorts.cli.prompts import ask_choice, ask_nonempty, ask_search_method
from paper_sorts.db.repositories import PaperSummary
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)
console = Console()

app = typer.Typer(help="Search the paper database.")


def _display_paper(paper: PaperSummary) -> None:
    """Print a paper's details in a human-readable format.

    :param paper: :class:`~paper_sorts.db.repositories.PaperSummary` to display.
    """
    authors_str = " and ".join(paper.authors)
    console.print(f"[bold]title:[/bold] {paper.title}")
    console.print(f"[bold]authors:[/bold] {authors_str}")
    console.print(f"[bold]summary:[/bold] {paper.contents}")
    console.print(f"[bold]bib entry:[/bold] {paper.bibtex}")


def _disambiguate(papers: list[PaperSummary]) -> PaperSummary:
    """Ask the user to pick one paper from a list of matches.

    :param papers: List of matching papers (len ≥ 2).
    :return: The user-selected paper.
    """
    options = [f"{p.title} (id={p.id})" for p in papers]
    options.append("(A)bort")
    idx = ask_choice(options, "Choose a paper")
    if idx == len(papers) + 1:
        raise typer.Exit()
    return papers[idx - 1]


def run_search(engine: Engine) -> None:
    """Run an interactive search dialog against the given engine.

    Prompts the user to choose author or title search, then displays results.
    If multiple papers match, asks the user to disambiguate.

    :param engine: Active SQLAlchemy engine.
    """
    method = ask_search_method()

    if method == "author":
        author = ask_nonempty("Author name")
        results = paper_service.search_by_author(engine, author)
        if not results:
            console.print(f"[yellow]Author {author!r} not found in database.[/yellow]")
            logger.info("Author %r not found", author)
            return
    else:
        title = ask_nonempty("Paper title")
        results = paper_service.search_by_title(engine, title)
        if not results:
            console.print(f"[yellow]Paper {title!r} not found in database.[/yellow]")
            logger.info("Paper title %r not found", title)
            return

    if len(results) == 1:
        paper = results[0]
    else:
        paper = _disambiguate(results)

    _display_paper(paper)


@app.callback(invoke_without_command=True)
def search_cmd(ctx: typer.Context) -> None:
    """Interactive search dialog.

    Prompts the user to choose author or title search, then displays results.
    If multiple papers match, asks the user to disambiguate.
    """
    if ctx.invoked_subcommand is not None:
        return
    engine: Engine = ctx.obj["engine"]
    run_search(engine)
