"""Search subcommand for pdbsearch.

Provides :func:`search_cmd` — a Typer command that lets the user search the
database by paper title or by author name.  Results are displayed to stdout.
The subcommand is registered on the main app in :mod:`paper_sorts.cli.app`.
"""

from __future__ import annotations

import logging

import typer

from paper_sorts.cli.prompts import ask_choice, ask_nonempty
from paper_sorts.db.repositories import PaperSummary
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)

app = typer.Typer(help="Search the paper database by title or author.")


def _display_paper(summary: PaperSummary) -> None:
    """Print a formatted paper summary to stdout.

    :param summary: Paper summary to display.
    """
    authors = ", ".join(summary.authors) if summary.authors else "(no authors)"
    print(f"\ntitle: {summary.title}")
    print(f"authors: {authors}")
    print(f"summary: {summary.summary}")
    print(f"bib entry: {summary.bibtex_text}")


def _pick_from_results(results: list[PaperSummary]) -> PaperSummary | None:
    """If multiple results, ask user to pick one; if one, return it directly.

    :param results: Non-empty list of matching papers.
    :returns: The chosen :class:`PaperSummary`, or ``None`` if the user aborts.
    """
    if len(results) == 1:
        return results[0]
    options = [f"{r.title} (id={r.paper_id})" for r in results]
    options.append("Abort — go back")
    choice = ask_choice(options, "Select a paper")
    if choice == len(options):
        return None
    return results[choice - 1]


def run_search(database_url: str) -> None:
    """Interactive search flow (called from interactive menu or subcommand).

    :param database_url: PostgreSQL DSN.
    """
    options = ["Search by title", "Search by author", "Back / Quit"]
    choice = ask_choice(options, "Search method")
    if choice == 3:
        return

    if choice == 1:
        title = ask_nonempty("Enter title")
        results = paper_service.search_by_title(
            title, database_url=database_url, with_session_fn=with_session
        )
        if not results:
            print("No papers found for that title.")
            return
        paper = _pick_from_results(results)
        if paper:
            _display_paper(paper)
    else:
        author = ask_nonempty("Enter author name (Last, First)")
        results = paper_service.search_by_author(
            author, database_url=database_url, with_session_fn=with_session
        )
        if not results:
            print("No papers found for that author.")
            return
        paper = _pick_from_results(results)
        if paper:
            _display_paper(paper)


@app.callback(invoke_without_command=True)
def search_cmd(
    ctx: typer.Context,
    database_url: str = typer.Option(
        None, "--database-url", envvar="PDBSEARCH_DATABASE_URL", help="PostgreSQL DSN"
    ),
) -> None:
    """Search the paper database by title or author."""
    if ctx.invoked_subcommand is not None:
        return
    if not database_url:
        typer.echo("Error: database URL not configured.", err=True)
        raise typer.Exit(1)
    run_search(database_url)
