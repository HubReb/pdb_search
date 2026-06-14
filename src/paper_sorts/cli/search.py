"""Search subcommands for paper_sorts CLI.

Provides search-by-title and search-by-author operations.  All prompts route
through cli/prompts.py (constitution Principle III).
"""

from __future__ import annotations

import logging

import typer
from rich.console import Console

from paper_sorts.cli.prompts import ask_int, ask_str, display_paper, display_papers_list
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)
app = typer.Typer(help="Search for papers by title or author.")
_console = Console()


@app.callback(invoke_without_command=True)
def search_callback(ctx: typer.Context) -> None:
    """Run search menu when invoked as subcommand with no further subcommand.

    Args:
        ctx: Typer context.
    """
    if ctx.invoked_subcommand is None:
        from paper_sorts.cli.app import get_database_url

        run_search_menu(get_database_url())


def run_search_menu(database_url: str) -> None:
    """Interactive search submenu.

    Args:
        database_url: SQLAlchemy connection string.
    """
    _console.print("\n[bold]Search[/bold]")
    _console.print("1) Search by title")
    _console.print("2) Search by author")
    _console.print("3) Back")
    choice = ask_int("Choice", [1, 2, 3])
    if choice == 1:
        _search_by_title(database_url)
    elif choice == 2:
        _search_by_author(database_url)
    # 3 = Back, return silently


def _search_by_title(database_url: str) -> None:
    """Prompt for a title query and display matching papers.

    Args:
        database_url: SQLAlchemy connection string.
    """
    title = ask_str("Enter title (or part of title)")
    try:
        with with_session(database_url) as session:
            results = paper_service.search_by_title(session, title)
    except Exception:
        logger.exception("search_by_title failed")
        _console.print("[red]Search failed. Check logs for details.[/red]")
        return

    if not results:
        _console.print("No papers found.")
        return

    if len(results) == 1:
        display_paper(results[0])
        return

    # Multiple matches — disambiguation
    _console.print(f"\nFound {len(results)} matching papers:")
    display_papers_list(results)
    _console.print(f"{len(results) + 1}) Back")
    choices = list(range(1, len(results) + 2))
    choice = ask_int("Select paper", choices)
    if choice == len(results) + 1:
        return
    display_paper(results[choice - 1])


def _search_by_author(database_url: str) -> None:
    """Prompt for an author name and display matching papers.

    Args:
        database_url: SQLAlchemy connection string.
    """
    author = ask_str("Enter author name (Last, First)")
    try:
        with with_session(database_url) as session:
            results = paper_service.search_by_author(session, author)
    except Exception:
        logger.exception("search_by_author failed")
        _console.print("[red]Search failed. Check logs for details.[/red]")
        return

    if not results:
        _console.print("No papers found.")
        return

    for result in results:
        _console.print(f"  [{result.bibtex_id}] {result.title}")
