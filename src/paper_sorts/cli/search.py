"""Search subcommand for pdbsearch CLI."""

import logging
import sys

import typer
from sqlalchemy.engine import Engine

from paper_sorts.cli.prompts import ask_choice, ask_input
from paper_sorts.db.repositories import PaperSummary
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)

app = typer.Typer(help="Search the paper database by title or author.")


def _format_paper(paper: PaperSummary) -> None:
    """Print a paper summary in the canonical pretty-print format.

    :param paper: :class:`~paper_sorts.db.repositories.PaperSummary` DTO.
    """
    authors_str = " and ".join(paper.authors) if paper.authors else "(no authors)"
    print(f"title: {paper.title}")
    print(f"authors: {authors_str}")
    print(f"summary: {paper.contents}")
    print(f"bib entry: {paper.bibtex}")


def _pick_paper(papers: list[PaperSummary]) -> PaperSummary | None:
    """Offer the user a numbered list and return the chosen paper.

    :param papers: Non-empty list of papers to choose from.
    :returns: The chosen :class:`~paper_sorts.db.repositories.PaperSummary`,
        or ``None`` if the user chose quit.
    """
    if len(papers) == 1:
        return papers[0]

    labels = [f"{p.title} (id={p.id})" for p in papers]
    idx = ask_choice(labels, prompt="Choose paper: ", quit_label="(Q)uit / abort")
    if idx == -1:
        return None
    return papers[idx]


def run_search(engine: Engine) -> None:
    """Interactive search flow — called from the top-level menu.

    :param engine: Active SQLAlchemy engine.
    """

    items = ["Search by author", "Search by paper title"]
    idx = ask_choice(items, prompt="Search method: ", quit_label="(Q)uit / abort")
    if idx == -1:
        return

    if idx == 0:
        _do_search_by_author(engine)
    else:
        _do_search_by_title(engine)


def _do_search_by_title(engine: Engine) -> None:
    """Execute the search-by-title flow.

    :param engine: Active SQLAlchemy engine.
    """

    title = ask_input("Please enter the paper title: ")
    results = paper_service.search_by_title(engine, title)
    if not results:
        print("Paper was not found in the database.")
        logger.info("No paper found for title '%s'", title)
        return

    paper = _pick_paper(results)
    if paper is None:
        return
    _format_paper(paper)


def _do_search_by_author(engine: Engine) -> None:
    """Execute the search-by-author flow.

    :param engine: Active SQLAlchemy engine.
    """

    author = ask_input("Please enter the author's name: ")
    results = paper_service.search_by_author(engine, author)
    if not results:
        print("Author was not found in the database.")
        logger.info("No paper found for author '%s'", author)
        return

    paper = _pick_paper(results)
    if paper is None:
        return
    _format_paper(paper)


@app.command()
def search_cmd(
    ctx: typer.Context,
    by: str | None = typer.Option(None, "--by", help="Search method: title or author"),
    query: str | None = typer.Option(None, "--query", help="Search term"),
) -> None:
    """Search the database interactively or with flags.

    :param ctx: Typer context carrying the engine.
    :param by: Optional search method (``title`` or ``author``).
    :param query: Optional search term.
    """
    raw_engine = ctx.obj.get("engine") if ctx.obj else None
    if raw_engine is None or not isinstance(raw_engine, Engine):
        logger.error("No database connection available")
        print("Error: no database URL configured.")
        sys.exit(1)
    engine: Engine = raw_engine

    if by is None and query is None:
        run_search(engine)
        return

    if by == "title":
        q = query or ask_input("Please enter the paper title: ")
        results = paper_service.search_by_title(engine, q)
        if not results:
            print("Paper was not found in the database.")
            return
        paper = _pick_paper(results)
        if paper:
            _format_paper(paper)
    elif by == "author":
        q = query or ask_input("Please enter the author's name: ")
        results = paper_service.search_by_author(engine, q)
        if not results:
            print("Author was not found in the database.")
            return
        paper = _pick_paper(results)
        if paper:
            _format_paper(paper)
    else:
        print(f"Unknown search method '{by}'. Use --by title or --by author.")
        sys.exit(1)
