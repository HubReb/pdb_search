"""Search subcommand for paper_sorts CLI."""

from __future__ import annotations

import logging

import typer

from paper_sorts.cli.prompts import ask_choice, ask_str, print_paper
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service

log = logging.getLogger(__name__)

app = typer.Typer(help="Search for papers by title or author.")


def _run_search(database_url: str) -> None:
    """Execute the interactive search flow.

    :param database_url: SQLAlchemy-compatible database URL.
    """
    choice = ask_choice(
        "Search interface",
        ["Search by author", "Search by paper title"],
    )
    if choice is None:
        raise typer.Exit()

    with with_session(database_url) as session:
        if choice == 1:
            author = ask_str("Author name")
            results = paper_service.search_by_author(session, author)
            if not results:
                typer.echo("Author not found in database.")
                return
            if len(results) == 1:
                p = results[0]
            else:
                sel = ask_choice(
                    "Multiple papers found — choose one",
                    [r.title for r in results],
                )
                if sel is None:
                    return
                p = results[sel - 1]
            print_paper(p.title, p.authors, p.contents, p.bibtex)
        else:
            title = ask_str("Paper title")
            results = paper_service.search_by_title(session, title)
            if not results:
                typer.echo("Paper not found in database.")
                return
            if len(results) == 1:
                p = results[0]
            else:
                sel = ask_choice(
                    "Multiple papers found — choose one",
                    [r.title for r in results],
                )
                if sel is None:
                    return
                p = results[sel - 1]
            print_paper(p.title, p.authors, p.contents, p.bibtex)
