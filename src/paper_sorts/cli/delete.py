"""Delete subcommand for paper_sorts CLI.

Allows the user to delete an existing paper after confirmation.
Constitution Principle III: destructive operations MUST present a confirmation
step summarising the exact change before it is applied.
"""

from __future__ import annotations

import logging

import typer
from rich.console import Console

from paper_sorts.cli.prompts import ask_confirm, ask_int, ask_str, display_paper
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)
app = typer.Typer(help="Delete a paper from the database.")
_console = Console()


@app.callback(invoke_without_command=True)
def delete_callback(
    ctx: typer.Context,
    paper_id: int | None = typer.Option(None, "--id", help="Paper ID to delete"),
) -> None:
    """Run delete flow when invoked as subcommand.

    Args:
        ctx: Typer context.
        paper_id: Optional paper ID; if absent, search first.
    """
    if ctx.invoked_subcommand is None:
        from paper_sorts.cli.app import get_database_url

        run_delete(get_database_url(), paper_id=paper_id)


def run_delete(database_url: str, paper_id: int | None = None) -> None:
    """Interactive delete paper flow.

    Args:
        database_url: SQLAlchemy connection string.
        paper_id: If provided, delete this paper directly; otherwise search first.
    """
    if paper_id is None:
        paper_id = _find_paper(database_url)
        if paper_id is None:
            return

    # Fetch and display current paper for confirmation
    try:
        with with_session(database_url) as session:
            from paper_sorts.db.repositories import PaperRepository

            repo = PaperRepository(session)
            current = repo.get_by_id(paper_id)
    except Exception:
        logger.exception("Failed to fetch paper id=%d for delete", paper_id)
        _console.print("[red]Could not fetch paper. Check logs.[/red]")
        return

    if current is None:
        _console.print(f"[red]No paper with id={paper_id}.[/red]")
        return

    _console.print("\n[bold]Paper to delete:[/bold]")
    display_paper(current)

    confirmed = ask_confirm(f'Delete "{current.title}" (id={paper_id})?')
    if not confirmed:
        _console.print("Delete aborted.")
        return

    try:
        with with_session(database_url) as session:
            paper_service.delete_paper(session, paper_id)
        _console.print("[green]Paper deleted successfully.[/green]")
    except Exception as exc:
        logger.exception("delete_paper failed for id=%d", paper_id)
        _console.print(f"[red]Delete failed: {exc}[/red]")


def _find_paper(database_url: str) -> int | None:
    """Search for a paper by title and return its id.

    Args:
        database_url: SQLAlchemy connection string.

    Returns:
        Paper id, or None if user aborted or no results found.
    """
    title = ask_str("Enter title to search for")
    try:
        with with_session(database_url) as session:
            results = paper_service.search_by_title(session, title)
    except Exception:
        logger.exception("search failed during delete")
        _console.print("[red]Search failed. Check logs.[/red]")
        return None

    if not results:
        _console.print("No papers found.")
        return None

    if len(results) == 1:
        return results[0].id

    from paper_sorts.cli.prompts import display_papers_list

    display_papers_list(results)
    _console.print(f"{len(results) + 1}) Abort")
    choices = list(range(1, len(results) + 2))
    choice = ask_int("Select paper to delete", choices)
    if choice == len(results) + 1:
        return None
    return results[choice - 1].id
