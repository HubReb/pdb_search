"""search subcommand for pdbsearch CLI.

Provides `pdbsearch search` (direct invocation) and `run_search`
(called from interactive menu). All prompts route through cli/prompts.py.
"""

from __future__ import annotations

import logging
from typing import Annotated

import typer

from paper_sorts.db.repositories import PaperSummary

logger = logging.getLogger(__name__)

app = typer.Typer()


def _display_paper(paper: PaperSummary) -> None:
    """Pretty-print a single paper to stdout.

    :param paper: PaperSummary DTO to display
    """
    print(f"\nTitle   : {paper.title}")
    print(f"Authors : {', '.join(paper.authors)}")
    print(f"BibTeX  : {paper.bibtex_id}")
    print(f"Summary : {paper.contents}")
    if paper.bibtex:
        print(f"\nBibTeX Entry:\n{paper.bibtex}")
    print()


def run_search(database_url: str) -> None:
    """Interactive search flow (called from the main menu).

    Prompts for search type and query, then displays results.

    :param database_url: PostgreSQL DSN
    """
    from paper_sorts.cli.prompts import ask_choice, ask_text
    from paper_sorts.services.paper_service import search_by_author, search_by_title

    method = ask_choice(
        "Search by:",
        ["Author name", "Paper title"],
        allow_quit=True,
    )
    if method is None:
        return

    query = ask_text("Enter search term")
    try:
        if method == "Author name":
            results = search_by_author(database_url, query)
        else:
            results = search_by_title(database_url, query)
    except KeyError as exc:
        print(f"No results: {exc}")
        logger.info("Search returned no results for '%s'", query)
        return
    except Exception as exc:
        logger.error("Search failed: %s", exc)
        print("Search failed — please check the logs.")
        return

    if not results:
        print(f"No results found for '{query}'.")
        return

    if len(results) == 1:
        _display_paper(results[0])
        return

    # Disambiguation
    from paper_sorts.cli.prompts import ask_choice as _ask_choice

    titles = [f"{p.title} ({p.bibtex_id})" for p in results]
    chosen_label = _ask_choice(
        f"Found {len(results)} matches. Select one:",
        titles,
        allow_quit=True,
    )
    if chosen_label is None:
        return
    idx = titles.index(chosen_label)
    _display_paper(results[idx])


@app.command("search")
def search_cmd(
    ctx: typer.Context,
    by: Annotated[
        str | None,
        typer.Option("--by", help="Search by: title or author"),
    ] = None,
    query: Annotated[
        str | None,
        typer.Option("--query", "-q", help="Search term"),
    ] = None,
) -> None:
    """Search the paper database by title or author.

    :param ctx: Typer context carrying settings from the app callback
    :param by: 'title' or 'author'; prompts if omitted
    :param query: search term; prompts if omitted
    """
    from paper_sorts.cli.prompts import ask_choice, ask_text
    from paper_sorts.services.paper_service import search_by_author, search_by_title

    settings = ctx.obj["settings"] if ctx.obj else None
    database_url: str
    if settings is not None:
        database_url = settings.get_database_url()
    else:
        database_url = typer.get_app_dir("pdbsearch")  # fallback; won't be used normally

    if by is None:
        method = ask_choice("Search by:", ["author", "title"], allow_quit=True)
        if method is None:
            return
        by = method

    if query is None:
        query = ask_text("Enter search term")

    try:
        if by.lower() == "author":
            results = search_by_author(database_url, query)
        else:
            results = search_by_title(database_url, query)
    except KeyError as exc:
        print(f"No results: {exc}")
        return
    except Exception as exc:
        logger.error("Search failed: %s", exc)
        print("Search failed — please check the logs.")
        raise typer.Exit(1) from exc

    if not results:
        print(f"No results found for '{query}'.")
        return

    if len(results) == 1:
        _display_paper(results[0])
        return

    titles = [f"{p.title} ({p.bibtex_id})" for p in results]
    chosen_label = ask_choice(
        f"Found {len(results)} matches. Select one:", titles, allow_quit=True
    )
    if chosen_label is None:
        return
    idx = titles.index(chosen_label)
    _display_paper(results[idx])
